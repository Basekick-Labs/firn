<p align="center">
  <img src="firn_logo.png" alt="Firn" width="180" />
</p>

<h1 align="center">Firn</h1>

<p align="center"><strong>The open source alternative to Amazon S3 Tables.</strong></p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License: Apache 2.0" /></a>
  <a href="https://github.com/Basekick-Labs/firn/releases"><img src="https://img.shields.io/github/v/release/Basekick-Labs/firn?color=brightgreen&label=release" alt="Latest release" /></a>
  <a href="https://github.com/Basekick-Labs/firn/actions"><img src="https://img.shields.io/github/actions/workflow/status/Basekick-Labs/firn/ci.yml?branch=main&label=CI" alt="CI" /></a>
  <a href="https://goreportcard.com/report/github.com/Basekick-Labs/firn"><img src="https://goreportcard.com/badge/github.com/Basekick-Labs/firn" alt="Go Report Card" /></a>
  <img src="https://img.shields.io/badge/built%20with-Go-00ADD8?logo=go" alt="Built with Go" />
</p>

---

Firn is a writer-agnostic, cloud-agnostic table maintenance daemon for Apache
Iceberg. It provides the automatic compaction, snapshot lifecycle management,
and orphan file cleanup that S3 Tables offers — without the AWS lock-in, without
the 20-30x cost premium, and without caring who wrote the data.

Any engine that writes standard Apache Iceberg tables works with Firn: Arc,
Apache Spark, Apache Flink, DuckDB, RisingWave, Trino, or anything else.

---

## Why Firn

Amazon S3 Tables solves a real problem: Iceberg tables accumulate small files,
stale snapshots, and orphaned data over time. Without maintenance, query
performance degrades and storage costs grow. S3 Tables fixes this with fully
automatic compaction and lifecycle management — but only on AWS, only at AWS
prices, and only on AWS terms.

The open source catalog ecosystem (Lakekeeper, Apache Polaris, Project Nessie)
solves table discovery and metadata management well. What it does not solve is
maintenance. Compaction, snapshot expiry, and orphan cleanup are left to the
user to orchestrate with external Spark clusters, Airflow DAGs, or manual
scripts. Firn closes that gap.

**Firn is the maintenance layer the open Iceberg ecosystem is missing.**

---

## What Firn Does

### Automatic Compaction
Small files are the primary cause of slow Iceberg query performance. Firn
continuously monitors registered tables and merges small files into larger,
optimally-sized ones using configurable strategies:

- **Binpack** — pack files to a target size (default 512 MB), no reordering
- **Sort** — merge and sort by specified columns for predicate pushdown gains
- **Z-order** — multi-dimensional sort for high-cardinality filter columns

### Snapshot Lifecycle Management
Every write to an Iceberg table creates a new snapshot. Without cleanup, metadata
grows unbounded and time-travel storage costs accumulate. Firn expires snapshots
according to configurable retention policies (by count, by age, or both) and
removes the manifest files that only expired snapshots reference.

### Orphan File Cleanup
Interrupted writes, failed jobs, and bug-induced partial commits leave orphaned
files in storage that no snapshot references. Firn identifies and removes them
safely, with a configurable grace period to avoid racing with active writers.

---

## What Firn Is Not

- **Not a catalog.** Use [Lakekeeper](https://github.com/lakekeeper/lakekeeper),
  Apache Polaris, or Project Nessie for table discovery and metadata.
- **Not a query engine.** Use DuckDB, Trino, Spark, or any Iceberg-compatible
  engine to query your tables.
- **Not a writer.** Use whatever engine fits your workload.

Firn does one thing: keep your Iceberg tables healthy.

---

## Architecture

```
Any Writer (Arc, Spark, DuckDB, Flink, RisingWave, ...)
        │
        │  commits Iceberg snapshots
        ▼
  Iceberg REST Catalog (Lakekeeper, Polaris, Nessie, ...)
        │
        │  table registry + metadata
        ▼
         Firn — Maintenance Daemon
        ├── Compaction engine
        │     ├── Candidate selection (reads Iceberg manifests)
        │     ├── DuckDB merge (subprocess-isolated)
        │     ├── Atomic snapshot commit (via catalog REST API)
        │     └── Crash recovery (pre-upload manifests)
        ├── Snapshot expiry
        │     ├── Retention policy evaluation
        │     ├── Manifest + data file GC
        │     └── Atomic metadata commit
        └── Orphan file cleanup
              ├── Storage enumeration
              ├── Live file reconciliation
              └── Safe deletion (grace period)
        │
        ▼
  Any S3-compatible backend
  (AWS S3, MinIO, Cloudflare R2, Tigris, Ceph, GCS, Azure Blob, ...)
```

---

## Design Principles

**Writer-agnostic.** Firn reads standard Iceberg metadata. It does not care
which engine wrote the data, what language it used, or what framework it runs on.

**Catalog-agnostic.** Firn starts with Lakekeeper (REST catalog, Rust, single
binary — the best open option today) and is designed to support any catalog that
implements the Iceberg REST Catalog spec.

**No JVM. No Spark. No Airflow.** Firn is a single Go binary. Deploying it
requires no cluster, no orchestration framework, and no external dependencies
beyond a catalog and object storage.

**Policy-driven.** Maintenance rules are declared per table or per namespace.
Firn evaluates them on a schedule and acts. No manual triggers required.

**Crash-safe.** Every compaction job writes a recovery manifest before uploading
output. On restart, Firn reconciles any interrupted jobs before starting new ones.

**Cloud-agnostic.** Firn speaks S3 API. Any S3-compatible object store works:
MinIO, Cloudflare R2, Tigris, Ceph, Wasabi, GCS (via interop), Azure Blob (via
interop). No AWS account required.

---

## Catalog Support

| Catalog | Status |
|---|---|
| [Lakekeeper](https://github.com/lakekeeper/lakekeeper) | ✅ Implemented |
| AWS Glue Data Catalog | ✅ Implemented |
| Apache Polaris | Planned (v0.2) |
| Project Nessie | Planned (v0.2) |

---

## Storage Backend Support

| Backend | Status |
|---|---|
| AWS S3 | ✅ Implemented |
| MinIO | ✅ Implemented |
| Cloudflare R2 | ✅ Implemented |
| Tigris | ✅ Implemented |
| Ceph (S3-compatible) | ✅ Implemented |
| Google Cloud Storage | Planned (v0.2) |
| Azure Blob Storage | Planned (v0.2) |

---

## Configuration

Firn is configured via a single YAML file.

```yaml
catalog:
  type: lakekeeper
  url: http://localhost:8080
  credential:
    client_id: firn
    client_secret: secret

storage:
  type: s3
  endpoint: http://localhost:9000   # omit for AWS
  region: us-east-1
  access_key_id: minioadmin
  secret_access_key: minioadmin
  path_style: true                  # required for MinIO

maintenance:
  # Default policy applied to all tables unless overridden
  defaults:
    compaction:
      enabled: true
      strategy: binpack             # binpack | sort | z-order
      target_file_size_mb: 512
      min_file_count: 5             # don't compact fewer than N files
      min_file_age_minutes: 60      # don't touch files written in last N minutes
    snapshot_expiry:
      enabled: true
      min_snapshots_to_keep: 5
      max_snapshot_age_hours: 120
    orphan_cleanup:
      enabled: true
      grace_period_hours: 24

  # Per-namespace overrides — only the specified fields are overridden;
  # unspecified fields inherit from defaults.
  # Note: namespace and table overrides each merge against the global defaults
  # independently — a table override does NOT layer on top of a namespace override.
  namespaces:
    analytics:
      compaction:
        strategy: sort
        sort_keys: ["event_time", "user_id"]
        target_file_size_mb: 1024

    # Disable all maintenance for the archive namespace.
    archive:
      compaction:
        enabled: false
      snapshot_expiry:
        enabled: false
      orphan_cleanup:
        enabled: false

  # Per-table overrides
  tables:
    analytics.events:
      compaction:
        strategy: z-order
        z_order_columns: ["user_id", "event_type"]

scheduler:
  interval: "5m"                    # how often Firn evaluates tables
  max_concurrent_jobs: 4
  memory_limit: "4GB"               # DuckDB memory cap per compaction job
```

---

## Deployment

### Docker

```bash
docker run -v ./firn.yaml:/etc/firn/config.yaml ghcr.io/basekick-labs/firn:latest
```

### Helm (Kubernetes)

```bash
helm repo add firn https://charts.firn.dev
helm install firn firn/firn -f values.yaml
```

### Binary

```bash
firn --config firn.yaml
```

---

## Arc Integration

[Arc](https://github.com/basekick-labs/arc) is the reference tight integration.
When Arc is configured with Firn, it:

- Registers new tables with the catalog automatically on first write
- Reports newly written files to Firn after each flush
- Delegates compaction entirely to Firn (Arc's internal compaction is disabled
  for Firn-managed tables)

Arc remains one writer among many. Any other Iceberg writer works without
any Arc involvement.

---

## Comparison

| | S3 Tables | Firn | Lakekeeper alone |
|---|---|---|---|
| Auto compaction | ✅ | ✅ | ❌ |
| Snapshot expiry | ✅ | ✅ | ❌ |
| Orphan cleanup | ✅ | ✅ | ❌ |
| REST Iceberg catalog | ✅ | via Lakekeeper | ✅ |
| Writer-agnostic | ✅ | ✅ | ✅ |
| Any S3-compatible | ❌ AWS only | ✅ | ✅ |
| Self-hostable | ❌ | ✅ | ✅ |
| No JVM | ✅ | ✅ | ✅ |
| Open source | ❌ | ✅ Apache 2.0 | ✅ Apache 2.0 |
| Cost | 20-30x premium | infrastructure only | infrastructure only |

---

## License

Apache License 2.0. See [LICENSE](LICENSE).

---

## Status

Firn is in active development (pre-v1.0). Compaction (binpack strategy, DuckDB
subprocess-isolated), snapshot expiry (retention by count + age, atomic removal),
and orphan file cleanup (grace period, full manifest-walk live-file reconciliation)
are implemented end-to-end. AWS Glue and Lakekeeper catalogs are supported.
S3-compatible storage and config-driven scheduling are working.

Not yet suitable for production use.

Open an issue or follow the project to contribute.
