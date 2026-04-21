# Firn Architecture

## Overview

Firn is a single Go binary that runs as a long-lived daemon. It polls registered
Iceberg tables on a configurable schedule, evaluates maintenance policies, and
executes compaction, snapshot expiry, and orphan cleanup jobs.

It does not embed a catalog. It connects to an external catalog (Lakekeeper,
Polaris, Nessie, or AWS Glue) to discover tables and commit new snapshots
atomically.

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
│  │   Backend   │    │  │ Orphan Cleanup     │  │   │
│  └─────────────┘    │  │  - Storage enum.   │  │   │
│                     │  │  - Live reconcile  │  │   │
│                     │  └────────────────────┘  │   │
│                     └──────────────────────────┘   │
└─────────────────────────────────────────────────────┘
```

---

## Core Interfaces

### catalog.Client

```go
type Client interface {
    ListNamespaces(ctx context.Context) ([]string, error)
    ListTables(ctx context.Context, namespace string) ([]TableIdentifier, error)
    LoadTable(ctx context.Context, id TableIdentifier) (*iceberg.TableMetadata, error)
    CommitTransaction(ctx context.Context, id TableIdentifier, tx Transaction) error
}
```

Implementations:
- `catalog/rest` — shared Iceberg REST client (OAuth2, pagination, commit); used by Lakekeeper, Polaris, and Nessie
- `catalog/lakekeeper` — thin wrapper: `/v1/...` paths, `{url}/oauth/tokens`
- `catalog/polaris` — thin wrapper: `/v1/...` paths, `{url}/oauth/tokens` (same as Lakekeeper)
- `catalog/nessie` — thin wrapper: `/iceberg/v1/...` paths, configurable token endpoint
- `catalog/glue` — AWS Glue Data Catalog (no REST, uses AWS SDK)

### storage.Backend

```go
type Backend interface {
    Read(ctx context.Context, path string) (io.ReadCloser, error)
    Write(ctx context.Context, path string, r io.Reader, size int64) error
    Delete(ctx context.Context, path string) error
    DeleteMany(ctx context.Context, paths []string) error
    Exists(ctx context.Context, path string) (bool, error)
    List(ctx context.Context, prefix string) ([]string, error)
    StatFile(ctx context.Context, path string) (int64, error)
    ModTime(ctx context.Context, path string) (time.Time, error)
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
   - Skip files written within `min_file_age_minutes`
   - Skip partitions with fewer than `min_file_count` files
   - Skip partitions already at or above `target_file_size_mb`
6. Return one `CompactionCandidate` per eligible partition group

### Compaction Execution

Compaction runs in a **subprocess** (child process, not goroutine). This is
required because DuckDB uses jemalloc which retains memory after `Close()`.
Running in a subprocess guarantees full memory release when the job finishes.

```
Parent process (Firn daemon)
    │
    ├── Serialises SubprocessConfig to stdin
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
    │       [ORDER BY sort_keys]          -- only for sort strategy
    │     ) TO output (FORMAT PARQUET, COMPRESSION ZSTD)
    ├── Upload compacted file to storage
    ├── Delete source files
    └── Write result to stdout, exit
```

Supported strategies: `binpack` (default), `sort` (requires `sort_keys`).

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
1. List all namespaces from the configured catalog
2. List all tables in each namespace
3. For each table, evaluate all three maintenance operations
4. Enforce `max_concurrent_jobs` across all tables with a semaphore

The scheduler is single-instance. In Kubernetes, run exactly one Firn pod per
catalog. Horizontal scaling is not needed — Firn is I/O-bound, not CPU-bound.

---

## Policy Engine

Each table resolves its effective policy by merging against the global defaults:

1. Start with the global `defaults` policy
2. If a namespace-level override exists, merge it (override fields win)
3. If a table-level override exists, merge it (override fields win)

Note: namespace and table overrides each merge directly against defaults — a
table override does NOT layer on top of a namespace override.

`enabled` uses `*bool` (pointer-to-bool) so that an absent YAML field (nil) is
distinguishable from an explicit `enabled: false`. This allows overrides to
disable a default-enabled policy without ambiguity.

---

## Observability

### Prometheus Metrics

Exposed at `GET /metrics` when `scheduler.metrics_addr` is set.

| Metric | Type | Labels |
|---|---|---|
| `firn_compaction_jobs_total` | Counter | `table`, `status` |
| `firn_compaction_files_merged_total` | Counter | `table` |
| `firn_compaction_bytes_read_total` | Counter | `table` |
| `firn_compaction_bytes_written_total` | Counter | `table` |
| `firn_compaction_duration_seconds` | Histogram | `table`, `status` |
| `firn_expiry_snapshots_expired_total` | Counter | `table` |
| `firn_expiry_manifests_deleted_total` | Counter | `table` |
| `firn_expiry_data_files_deleted_total` | Counter | `table` |
| `firn_expiry_duration_seconds` | Histogram | `table`, `status` |
| `firn_orphan_files_scanned_total` | Counter | `table` |
| `firn_orphan_files_deleted_total` | Counter | `table` |
| `firn_orphan_files_skipped_total` | Counter | `table` |
| `firn_orphan_duration_seconds` | Histogram | `table`, `status` |
| `firn_cycle_duration_seconds` | Histogram | — |
| `firn_cycle_tables_total` | Gauge | — |

Standard Go runtime and process metrics (`go_*`, `process_*`) are also included.

### Health Endpoint

`GET /healthz` — returns HTTP 200. Used for Kubernetes liveness probes.

---

## Module Structure

```
firn/
├── cmd/
│   ├── firn/           # Main daemon entrypoint
│   └── compact/        # Subprocess entrypoint (firn compact --job-stdin)
├── internal/
│   ├── catalog/        # Catalog client interface + implementations
│   │   ├── catalog.go  # Client interface, types (TableIdentifier, Transaction, etc.)
│   │   ├── rest/       # Shared Iceberg REST client (OAuth2, pagination, commit)
│   │   ├── lakekeeper/ # Thin wrapper over rest.Client
│   │   ├── polaris/    # Thin wrapper over rest.Client
│   │   ├── nessie/     # Thin wrapper over rest.Client (/iceberg prefix)
│   │   └── glue/       # AWS Glue Data Catalog client
│   ├── storage/        # Storage backend interface + implementations
│   │   └── s3/         # S3-compatible backend (AWS S3, MinIO, R2, Tigris, ...)
│   ├── compaction/     # Compaction engine
│   │   ├── compaction.go  # Candidate selection + FindCandidates
│   │   ├── execute.go     # ExecuteJob, subprocess launch
│   │   └── recovery.go    # Crash recovery manifest read/write/reconcile
│   ├── expiry/         # Snapshot expiry engine
│   ├── orphan/         # Orphan file cleanup engine
│   ├── policy/         # Policy merge + evaluation
│   ├── scheduler/      # Scheduler loop + buildCatalog/buildStorage
│   ├── metrics/        # Prometheus registry wrapper
│   ├── iceberg/        # Iceberg metadata types + Avro manifest parsing
│   └── config/         # Config loading + validation (YAML)
├── docs/
└── README.md
```
