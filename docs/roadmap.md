# Firn Roadmap

## v0.1 — Foundation

Goal: compaction working end-to-end against Lakekeeper + MinIO. Proves the
core loop. Not production-ready but demonstrable.

- [x] Go module scaffold (`cmd/firn`, `cmd/compact`, `internal/...`)
- [x] Storage backend: S3-compatible (AWS S3, MinIO, R2)
- [x] Iceberg metadata parsing: `metadata.json`, manifest list, manifests (Avro)
- [x] Catalog client: Lakekeeper REST API (table load, transaction commit)
- [ ] Catalog client: AWS Glue (Iceberg REST endpoint)
- [x] Compaction candidate selection from Iceberg manifests
- [x] DuckDB compaction query (binpack strategy, subprocess-isolated)
- [x] Crash-recovery manifest (write before upload, recover on restart)
- [x] Atomic snapshot commit via catalog
- [x] Config file loading (YAML)
- [x] Scheduler loop (fixed interval)
- [x] Basic logging (structured, zerolog)
- [ ] Docker image

## v0.2 — Maintenance Complete

Goal: full S3 Tables feature parity for maintenance operations.

- [ ] Snapshot expiry (retention by count + age)
- [ ] Orphan file cleanup (grace period, batch delete)
- [ ] Sort compaction strategy (ORDER BY sort keys)
- [ ] Z-order compaction strategy
- [ ] Adaptive batch splitting on OOM/segfault
- [ ] Per-namespace policy overrides
- [ ] Per-table policy overrides
- [ ] Catalog support: Apache Polaris, Project Nessie
- [ ] Storage backend: Google Cloud Storage, Azure Blob Storage
- [ ] Prometheus metrics endpoint
- [ ] Health + status HTTP endpoints
- [ ] Helm chart

## v0.3 — Operability

Goal: production-ready deployment experience.

- [ ] Policy stored as Iceberg table properties (no restart required)
- [ ] Job history persistence (survives restart)
- [ ] Configurable retry with backoff on catalog conflicts
- [ ] Dry-run mode (plan what would be compacted, don't execute)
- [ ] Manual trigger API (`POST /trigger/{namespace}/{table}`)
- [ ] Arc tight integration (Arc registers tables + delegates compaction to Firn)
- [ ] Multi-catalog support (single Firn instance, multiple catalogs)

## v0.4 — Scale

Goal: handles large table counts and high write-rate tables gracefully.

- [ ] Incremental manifest scanning (only scan new manifests since last run)
- [ ] Priority queuing (tables with most small files first)
- [ ] Partial compaction (compact subset of partitions per cycle)
- [ ] Configurable compaction window (only run during off-peak hours)
- [ ] Distributed mode (multiple Firn instances with work distribution)
