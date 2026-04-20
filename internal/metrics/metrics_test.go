package metrics

import (
	"errors"
	"testing"
	"time"

	"github.com/basekick-labs/firn/internal/compaction"
	"github.com/basekick-labs/firn/internal/expiry"
	"github.com/basekick-labs/firn/internal/orphan"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func newTestRegistry() (*Registry, *prometheus.Registry) {
	promReg := prometheus.NewRegistry()
	return NewRegistry(promReg), promReg
}

func counterValue(t *testing.T, reg *prometheus.Registry, name string, labels prometheus.Labels) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if labelsMatch(m.GetLabel(), labels) {
				if m.GetCounter() != nil {
					return m.GetCounter().GetValue()
				}
				if m.GetGauge() != nil {
					return m.GetGauge().GetValue()
				}
			}
		}
	}
	return 0
}

func labelsMatch(pairs []*dto.LabelPair, want prometheus.Labels) bool {
	got := make(map[string]string, len(pairs))
	for _, p := range pairs {
		got[p.GetName()] = p.GetValue()
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

func TestRecordCompaction_Success(t *testing.T) {
	reg, promReg := newTestRegistry()
	result := compaction.Result{
		InputFiles:  []string{"a.parquet", "b.parquet", "c.parquet"},
		BytesBefore: 3000,
		BytesAfter:  1000,
		Duration:    500 * time.Millisecond,
	}
	reg.RecordCompaction("ns.tbl", result, nil)

	assert.Equal(t, 1.0, counterValue(t, promReg, "firn_compaction_jobs_total", prometheus.Labels{"table": "ns.tbl", "status": "success"}))
	assert.Equal(t, 3.0, counterValue(t, promReg, "firn_compaction_files_merged_total", prometheus.Labels{"table": "ns.tbl"}))
	assert.Equal(t, 3000.0, counterValue(t, promReg, "firn_compaction_bytes_read_total", prometheus.Labels{"table": "ns.tbl"}))
	assert.Equal(t, 1000.0, counterValue(t, promReg, "firn_compaction_bytes_written_total", prometheus.Labels{"table": "ns.tbl"}))
}

func TestRecordCompaction_Error(t *testing.T) {
	reg, promReg := newTestRegistry()
	reg.RecordCompaction("ns.tbl", compaction.Result{Duration: 100 * time.Millisecond}, errors.New("boom"))

	assert.Equal(t, 1.0, counterValue(t, promReg, "firn_compaction_jobs_total", prometheus.Labels{"table": "ns.tbl", "status": "error"}))
	// file/byte counters must NOT be incremented on error
	assert.Equal(t, 0.0, counterValue(t, promReg, "firn_compaction_files_merged_total", prometheus.Labels{"table": "ns.tbl"}))
	assert.Equal(t, 0.0, counterValue(t, promReg, "firn_compaction_bytes_read_total", prometheus.Labels{"table": "ns.tbl"}))
}

func TestRecordCompaction_MultipleJobs(t *testing.T) {
	reg, promReg := newTestRegistry()
	for i := 0; i < 5; i++ {
		reg.RecordCompaction("ns.tbl", compaction.Result{
			InputFiles:  []string{"a.parquet"},
			BytesBefore: 100,
			BytesAfter:  80,
			Duration:    time.Second,
		}, nil)
	}
	assert.Equal(t, 5.0, counterValue(t, promReg, "firn_compaction_jobs_total", prometheus.Labels{"table": "ns.tbl", "status": "success"}))
	assert.Equal(t, 5.0, counterValue(t, promReg, "firn_compaction_files_merged_total", prometheus.Labels{"table": "ns.tbl"}))
}

func TestRecordExpiry_Success(t *testing.T) {
	reg, promReg := newTestRegistry()
	result := expiry.Result{
		ExpiredSnapshots: 3,
		DeletedManifests: 5,
		DeletedDataFiles: 10,
		Duration:         2 * time.Second,
	}
	reg.RecordExpiry("ns.tbl", result, nil)

	assert.Equal(t, 3.0, counterValue(t, promReg, "firn_expiry_snapshots_expired_total", prometheus.Labels{"table": "ns.tbl"}))
	assert.Equal(t, 5.0, counterValue(t, promReg, "firn_expiry_manifests_deleted_total", prometheus.Labels{"table": "ns.tbl"}))
	assert.Equal(t, 10.0, counterValue(t, promReg, "firn_expiry_data_files_deleted_total", prometheus.Labels{"table": "ns.tbl"}))
}

func TestRecordExpiry_Error(t *testing.T) {
	reg, promReg := newTestRegistry()
	reg.RecordExpiry("ns.tbl", expiry.Result{Duration: time.Second}, errors.New("fail"))
	// counters must not increment on error
	assert.Equal(t, 0.0, counterValue(t, promReg, "firn_expiry_snapshots_expired_total", prometheus.Labels{"table": "ns.tbl"}))
}

func TestRecordOrphan_Success(t *testing.T) {
	reg, promReg := newTestRegistry()
	result := orphan.Result{
		ScannedFiles: 100,
		DeletedFiles: 5,
		SkippedFiles: 3,
		Duration:     10 * time.Second,
	}
	reg.RecordOrphan("ns.tbl", result, nil)

	assert.Equal(t, 100.0, counterValue(t, promReg, "firn_orphan_files_scanned_total", prometheus.Labels{"table": "ns.tbl"}))
	assert.Equal(t, 5.0, counterValue(t, promReg, "firn_orphan_files_deleted_total", prometheus.Labels{"table": "ns.tbl"}))
	assert.Equal(t, 3.0, counterValue(t, promReg, "firn_orphan_files_skipped_total", prometheus.Labels{"table": "ns.tbl"}))
}

func TestRecordOrphan_Error(t *testing.T) {
	reg, promReg := newTestRegistry()
	reg.RecordOrphan("ns.tbl", orphan.Result{Duration: time.Second}, errors.New("fail"))
	assert.Equal(t, 0.0, counterValue(t, promReg, "firn_orphan_files_deleted_total", prometheus.Labels{"table": "ns.tbl"}))
}

func TestRecordCycle(t *testing.T) {
	reg, promReg := newTestRegistry()
	reg.RecordCycle(30*time.Second, 12)

	assert.Equal(t, 12.0, counterValue(t, promReg, "firn_cycle_tables_total", prometheus.Labels{}))
	// Verify the cycle duration histogram has one observation.
	count := testutil.CollectAndCount(promReg, "firn_cycle_duration_seconds")
	assert.Equal(t, 1, count)
}

func TestRecordCompaction_HistogramObserved(t *testing.T) {
	reg, promReg := newTestRegistry()
	reg.RecordCompaction("ns.tbl", compaction.Result{
		InputFiles: []string{"a.parquet"},
		Duration:   250 * time.Millisecond,
	}, nil)

	mfs, err := promReg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "firn_compaction_duration_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			if labelsMatch(m.GetLabel(), prometheus.Labels{"table": "ns.tbl", "status": "success"}) {
				assert.Equal(t, uint64(1), m.GetHistogram().GetSampleCount())
				return
			}
		}
	}
	t.Fatal("firn_compaction_duration_seconds{table=ns.tbl,status=success} not found")
}

func TestNilRegistrySafe(t *testing.T) {
	var reg *Registry
	// All methods must be no-ops on nil receiver.
	assert.NotPanics(t, func() {
		reg.RecordCompaction("t", compaction.Result{}, nil)
		reg.RecordExpiry("t", expiry.Result{}, nil)
		reg.RecordOrphan("t", orphan.Result{}, nil)
		reg.RecordCycle(time.Second, 1)
	})
}
