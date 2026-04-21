# Firn Architecture

## Overview

Firn is a single Go binary that runs as a long-lived daemon. It polls registered
Iceberg tables on a configurable schedule, evaluates maintenance policies, and
executes compaction, snapshot expiry, and orphan cleanup jobs.

It does not embed a catalog. It connects to an external catalog (Lakekeeper,
Polaris, Nessie) via the standard Iceberg REST Catalog API to discover tables
and commit new snapshots atomically.

---

## Component Map

```
┌─────────────────────────────────────────────────────┐
│                      Firn                           │
│                                                     │
│  ┌─────────────┐    ┌──────────────────────────┐   │
│  │  Scheduler  │───▶│    Maintenance Engine     │   │
│  └─────────────┘    │                          │   │
│                     │  ┌────────────────────┐  │   │
│  ┌─────────────┐    │  │ Compaction         │  │   │
│  │   Policy    │───▶│  │  - Candidate sel.  │  │   │
│  │   Engine    │    │  │  - DuckDB merge    │  │   │
│  └─────────────┘    │  │  - Snapshot commit │  │   │
│                     │  │  - Crash recovery  │  │   │
│  ┌─────────────┐    │  └────────────────────┘  │   │
│  │   Catalog   │◀──▶│  ┌────────────────────┐  │   │
│  │   Client    │    │  │ Snapshot Expiry    │  │   │
│  └─────────────┘    │  │  - Retention eval  │  │   │
│                     │  │  - Manifest GC     │  │   │
│  ┌─────────────┐    │  └────────────────────┘  │   │
│  │   Storage   │◀──▶│  ┌────────────────────┐  │   │
│  │   Client    │    │  │ Orphan Cleanup     │  │   │
│  └─────────────┘    │  │  - Storage enum.   │  │   │
│                     │  │  - Live reconcile  │  │   │
│                     │  └────────────────────┘  │   │
│                     └──────────────────────────┘   │
└─────────────────────────────────────────────────────┘
```

---

## Core Interfaces

### TableSource

The central abstraction. Every maintenance operation works through this interface.
Implementations exist for each catalog type.

```go
type TableSource interface {
    // ListTables returns all tables this source manages.
    ListTables(ctx context.Context) ([]TableIdentifier, error)

    // FindCompactionCandidates returns file groups ready for compaction.
    FindCompactionCandidates(ctx context.Context, table TableIdentifier, policy CompactionPolicy) ([]CompactionCandidate, error)

    // CommitCompaction atomically commits a compaction result as a new snapshot.
    CommitCompaction(ctx context.Context, table TableIdentifier, job CompactionResult) error

    // FindExpiredSnapshots returns snapshots eligible for expiry per policy.
    FindExpiredSnapshots(ctx context.Context, table TableIdentifier, policy SnapshotPolicy) ([]SnapshotExpiry, error)

    // CommitSnapshotExpiry removes expired snapshots and their exclusive files.
    CommitSnapshotExpiry(ctx context.Context, table TableIdentifier, expiry []SnapshotExpiry) error

    // ListOrphanFiles returns files in storage unreferenced by any snapshot.
    ListOrphanFiles(ctx context.Context, table TableIdentifier, gracePeriod time.Duration) ([]string, error)

    // DeleteOrphanFiles removes orphaned files from storage.
    DeleteOrphanFiles(ctx context.Context, table TableIdentifier, files []string) error
}
```

### StorageBackend

```go
type StorageBackend interface {
    Read(ctx context.Context, path string) (io.ReadCloser, error)
    ReadTo(ctx context.Context, path string, w io.Writer) error
    Write(ctx context.Context, path string, r io.Reader, size int64) error
    Delete(ctx context.Context, path string) error
    Exists(ctx context.Context, path string) (bool, error)
    List(ctx context.Context, prefix string) ([]string, error)
    StatFile(ctx context.Context, path string) (int64, error)
    ModTime(ctx context.Context, path string) (time.Time, error)
}
```

### CatalogClient

```go
type CatalogClient interface {
    ListNamespaces(ctx context.Context) ([]string, error)
    ListTables(ctx context.Context, namespace string) ([]TableIdentifier, error)
    LoadTable(ctx context.Context, id TableIdentifier) (*TableMetadata, error)
    CommitTransaction(ctx context.Context, id TableIdentifier, tx Transaction) error
}
```

---

## Compaction Engine

### Candidate Selection

1. Load current table metadata via catalog client
2. Identify the current snapshot
3. Walk manifest list → manifests → data file entries
4. Group data files by partition spec values
5. Apply policy filters:
   - Skip files written within `min_file_age`
   - Skip partitions with fewer than `min_file_count` files
   - Skip partitions already at or above `target_file_size`
6. Return one `CompactionCandidate` per eligible partition group

### Compaction Execution

Compaction runs in a **subprocess** (child process, not goroutine). This is
required because DuckDB uses jemalloc which retains memory after `Close()`.
Running in a subprocess guarantees full memory release when the job finishes.

```
Parent process (Firn daemon)
    │
    ├── Writes SubprocessConfig to stdin
    ├── Spawns: firn compact --job-stdin
    └── Reads SubprocessResult from stdout

Child process (firn compact)
    ├── Read config from stdin
    ├── Initialize storage backend
    ├── Initialize in-process DuckDB (SET memory_limit)
    ├── Download input files (parallel, streaming)
    ├── Validate Parquet magic bytes
    ├── Write crash-recovery manifest to storage
    ├── Execute compaction query:
    │     COPY (
    │       SELECT * FROM read_parquet([files], union_by_name=true)
    │       ORDER BY sort_keys
    │     ) TO output (FORMAT PARQUET, COMPRESSION ZSTD)
    ├── Upload compacted file to storage
    ├── Delete source files
    └── Write result to stdout, exit
```

### Crash Recovery

Before uploading the compacted output, the child process writes a recovery
manifest to storage:

```
{table-location}/_firn/compaction/{job-id}.json
{
  "job_id": "...",
  "output_path": "...",
  "output_size": 1073741824,
  "input_files": ["...", "..."],
  "snapshot_id": 8234729384,
  "status": "pending",
  "created_at": "2026-04-18T14:00:00Z"
}
```

On startup (and before each compaction cycle), Firn recovers orphaned manifests:

| State | Action |
|---|---|
| Manifest exists, output missing | Delete manifest, retry compaction |
| Manifest exists, output size mismatch | Delete output + manifest, retry |
| Manifest exists, output valid | Delete input files, commit snapshot, delete manifest |

### Atomic Snapshot Commit

After upload and source deletion succeed, Firn commits a new Iceberg snapshot
via the catalog's transaction API. The commit includes:

1. A new data manifest listing the compacted output file
2. A new manifest list referencing all surviving manifests plus the new one
3. A new snapshot object with incremented sequence number

The catalog enforces optimistic concurrency. If another writer committed between
the time Firn read the current snapshot and the time it attempts to commit, the
catalog returns a conflict error and Firn retries from candidate selection.

### Adaptive Batch Splitting

If a compaction job fails with a recoverable error (OOM, DuckDB segfault), Firn
splits the file batch in half and retries each half independently. This recurses
up to 4 levels (e.g., 30 → 15 → 7 → 3 files).

Recoverable errors: SIGKILL (exit 137), SIGSEGV (exit 139), out-of-memory log lines.
Non-recoverable errors: permission denied, file not found, access denied.

---

## Snapshot Expiry

1. Load all snapshots from table metadata
2. Evaluate retention policy:
   - `min_snapshots_to_keep` — never expire below this count
   - `max_snapshot_age_hours` — expire snapshots older than this
   - Both conditions must allow expiry before a snapshot is removed
3. Identify manifests referenced exclusively by expired snapshots
4. Identify data files referenced exclusively by those manifests
5. Build new metadata without expired snapshots
6. Commit new metadata via catalog
7. Delete expired manifest files and data files from storage

---

## Orphan File Cleanup

1. List all files under the table's storage prefix
2. Walk all snapshots → all manifests → all referenced data files
3. Subtract referenced files from storage listing
4. Filter by grace period: only delete files older than `grace_period_hours`
   (protects against racing with active writers)
5. Delete remaining files in batches

---

## Scheduling

Firn runs a scheduler loop at the configured interval (default: 5 minutes).

Each tick:
1. Recover orphaned compaction manifests
2. List all tables from all configured catalogs
3. For each table, evaluate all maintenance operations concurrently
4. Enforce `max_concurrent_jobs` across all tables
5. Record results in job history (last 100 jobs)

The scheduler is single-instance. In Kubernetes, run exactly one Firn pod per
catalog. Horizontal scaling is not needed — Firn is I/O-bound, not CPU-bound.

---

## Policy Engine

Policies are evaluated in this order (most specific wins):

1. Table-level policy (if defined)
2. Namespace-level policy (if defined)
3. Global default policy

Policies are loaded from the config file at startup. A future version will
support storing policies as Iceberg table properties for per-table overrides
without restarting Firn.

---

## Observability

### Metrics (Prometheus)

```
firn_compaction_jobs_total{table, status}
firn_compaction_files_merged_total{table}
firn_compaction_bytes_before_total{table}
firn_compaction_bytes_after_total{table}
firn_snapshots_expired_total{table}
firn_orphan_files_deleted_total{table}
firn_job_duration_seconds{table, operation}
firn_scheduler_cycle_duration_seconds
firn_catalog_request_duration_seconds{catalog, operation}
```

### Health Endpoint

`GET /healthz` — returns 200 if the daemon is running and catalog is reachable.

### Status Endpoint

`GET /status` — returns JSON with recent job history, current cycle state, and
per-table maintenance stats.

---

## Module Structure

```
firn/
├── cmd/
│   ├── firn/          # Main daemon entrypoint
│   └── compact/       # Subprocess entrypoint (firn compact --job-stdin)
├── internal/
│   ├── catalog/       # Catalog client interface + implementations
│   │   ├── rest/      # Shared Iceberg REST catalog client (OAuth2, pagination, commit)
│   │   ├── lakekeeper/
│   │   ├── polaris/
│   │   ├── nessie/
│   │   └── glue/
│   ├── storage/       # Storage backend interface + implementations
│   │   ├── s3/
│   │   ├── local/
│   │   └── azure/
│   ├── compaction/    # Compaction engine
│   │   ├── candidate.go
│   │   ├── job.go
│   │   ├── subprocess.go
│   │   ├── commit.go
│   │   ├── recovery.go
│   │   └── adaptive.go
│   ├── expiry/        # Snapshot expiry
│   ├── orphan/        # Orphan file cleanup
│   ├── policy/        # Policy evaluation engine
│   ├── scheduler/     # Main scheduler loop
│   ├── iceberg/       # Iceberg metadata types + parsing
│   │   ├── metadata.go
│   │   ├── manifest.go
│   │   └── snapshot.go
│   └── config/        # Config loading + validation
├── docs/
└── README.md
```
