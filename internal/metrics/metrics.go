package metrics

import (
	"time"

	"github.com/basekick-labs/firn/internal/compaction"
	"github.com/basekick-labs/firn/internal/expiry"
	"github.com/basekick-labs/firn/internal/orphan"
	"github.com/prometheus/client_golang/prometheus"
)

var durationBuckets = []float64{
	.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300,
}

// Registry holds all Firn Prometheus metrics and records observations.
// A nil *Registry is safe to use — all methods are no-ops.
type Registry struct {
	compactionJobs     *prometheus.CounterVec
	compactionFiles    *prometheus.CounterVec
	compactionBytesIn  *prometheus.CounterVec
	compactionBytesOut *prometheus.CounterVec
	compactionDuration *prometheus.HistogramVec

	expirySnapshots  *prometheus.CounterVec
	expiryManifests  *prometheus.CounterVec
	expiryDataFiles  *prometheus.CounterVec
	expiryDuration   *prometheus.HistogramVec

	orphanScanned  *prometheus.CounterVec
	orphanDeleted  *prometheus.CounterVec
	orphanSkipped  *prometheus.CounterVec
	orphanDuration *prometheus.HistogramVec

	cycleDuration prometheus.Histogram
	cycleTables   prometheus.Gauge
}

// NewRegistry registers all Firn metrics with reg and returns a Registry.
func NewRegistry(reg prometheus.Registerer) *Registry {
	r := &Registry{
		compactionJobs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "firn_compaction_jobs_total",
			Help: "Total compaction jobs attempted.",
		}, []string{"table", "status"}),
		compactionFiles: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "firn_compaction_files_merged_total",
			Help: "Total input files merged by compaction.",
		}, []string{"table"}),
		compactionBytesIn: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "firn_compaction_bytes_read_total",
			Help: "Total bytes read (before compaction).",
		}, []string{"table"}),
		compactionBytesOut: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "firn_compaction_bytes_written_total",
			Help: "Total bytes written (after compaction).",
		}, []string{"table"}),
		compactionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "firn_compaction_duration_seconds",
			Help:    "Duration of individual compaction jobs.",
			Buckets: durationBuckets,
		}, []string{"table", "status"}),

		expirySnapshots: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "firn_expiry_snapshots_expired_total",
			Help: "Total Iceberg snapshots expired.",
		}, []string{"table"}),
		expiryManifests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "firn_expiry_manifests_deleted_total",
			Help: "Total manifest files deleted during snapshot expiry.",
		}, []string{"table"}),
		expiryDataFiles: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "firn_expiry_data_files_deleted_total",
			Help: "Total data files deleted during snapshot expiry.",
		}, []string{"table"}),
		expiryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "firn_expiry_duration_seconds",
			Help:    "Duration of snapshot expiry runs per table.",
			Buckets: durationBuckets,
		}, []string{"table"}),

		orphanScanned: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "firn_orphan_files_scanned_total",
			Help: "Total files scanned during orphan cleanup.",
		}, []string{"table"}),
		orphanDeleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "firn_orphan_files_deleted_total",
			Help: "Total orphan files deleted.",
		}, []string{"table"}),
		orphanSkipped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "firn_orphan_files_skipped_total",
			Help: "Total files skipped during orphan cleanup (within grace period).",
		}, []string{"table"}),
		orphanDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "firn_orphan_duration_seconds",
			Help:    "Duration of orphan cleanup runs per table.",
			Buckets: durationBuckets,
		}, []string{"table"}),

		cycleDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "firn_cycle_duration_seconds",
			Help:    "Duration of full maintenance cycles.",
			Buckets: durationBuckets,
		}),
		cycleTables: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "firn_cycle_tables_total",
			Help: "Number of tables processed in the last maintenance cycle.",
		}),
	}

	reg.MustRegister(
		r.compactionJobs,
		r.compactionFiles,
		r.compactionBytesIn,
		r.compactionBytesOut,
		r.compactionDuration,
		r.expirySnapshots,
		r.expiryManifests,
		r.expiryDataFiles,
		r.expiryDuration,
		r.orphanScanned,
		r.orphanDeleted,
		r.orphanSkipped,
		r.orphanDuration,
		r.cycleDuration,
		r.cycleTables,
	)

	return r
}

func status(err error) string {
	if err != nil {
		return "error"
	}
	return "success"
}

// RecordCompaction records metrics for a single compaction job.
func (r *Registry) RecordCompaction(table string, result compaction.Result, err error) {
	if r == nil {
		return
	}
	st := status(err)
	r.compactionJobs.WithLabelValues(table, st).Inc()
	r.compactionDuration.WithLabelValues(table, st).Observe(result.Duration.Seconds())
	if err == nil {
		r.compactionFiles.WithLabelValues(table).Add(float64(len(result.InputFiles)))
		r.compactionBytesIn.WithLabelValues(table).Add(float64(result.BytesBefore))
		r.compactionBytesOut.WithLabelValues(table).Add(float64(result.BytesAfter))
	}
}

// RecordExpiry records metrics for a snapshot expiry run.
func (r *Registry) RecordExpiry(table string, result expiry.Result, err error) {
	if r == nil {
		return
	}
	r.expiryDuration.WithLabelValues(table).Observe(result.Duration.Seconds())
	if err == nil {
		r.expirySnapshots.WithLabelValues(table).Add(float64(result.ExpiredSnapshots))
		r.expiryManifests.WithLabelValues(table).Add(float64(result.DeletedManifests))
		r.expiryDataFiles.WithLabelValues(table).Add(float64(result.DeletedDataFiles))
	}
}

// RecordOrphan records metrics for an orphan cleanup run.
func (r *Registry) RecordOrphan(table string, result orphan.Result, err error) {
	if r == nil {
		return
	}
	r.orphanDuration.WithLabelValues(table).Observe(result.Duration.Seconds())
	if err == nil {
		r.orphanScanned.WithLabelValues(table).Add(float64(result.ScannedFiles))
		r.orphanDeleted.WithLabelValues(table).Add(float64(result.DeletedFiles))
		r.orphanSkipped.WithLabelValues(table).Add(float64(result.SkippedFiles))
	}
}

// RecordCycle records metrics for a full maintenance cycle.
func (r *Registry) RecordCycle(duration time.Duration, tables int) {
	if r == nil {
		return
	}
	r.cycleDuration.Observe(duration.Seconds())
	r.cycleTables.Set(float64(tables))
}
