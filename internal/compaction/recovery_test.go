package compaction

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/basekick-labs/firn/internal/catalog"
	"github.com/basekick-labs/firn/internal/iceberg"
	"github.com/basekick-labs/firn/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var tableID = catalog.TableIdentifier{Namespace: "ns", Name: "tbl"}

func newMeta(snapshotID int64) *iceberg.TableMetadata {
	meta := &iceberg.TableMetadata{
		Location:          "s3://bucket/ns/tbl",
		CurrentSnapshotID: snapshotID,
	}
	if snapshotID != 0 {
		meta.Snapshots = []iceberg.Snapshot{
			{SnapshotID: snapshotID, TimestampMs: time.Now().UnixMilli()},
		}
	}
	return meta
}

// --- write / read / delete round-trip ---

func TestRecovery_WriteReadDelete(t *testing.T) {
	stor := testutil.NewMemStorage(nil)
	rm := RecoveryManifest{
		JobID:      "job1",
		Table:      "ns.tbl",
		InputFiles: []string{"a.parquet", "b.parquet"},
		OutputFile: "out.parquet",
		State:      recoveryStatePending,
		CreatedAt:  time.Now().UTC().Format(time.RFC3339),
	}

	require.NoError(t, writeRecovery(context.Background(), stor, "recovery/job1.json", rm))

	got, err := readRecovery(context.Background(), stor, "recovery/job1.json")
	require.NoError(t, err)
	assert.Equal(t, "job1", got.JobID)
	assert.Equal(t, recoveryStatePending, got.State)
	assert.Equal(t, []string{"a.parquet", "b.parquet"}, got.InputFiles)

	require.NoError(t, deleteRecovery(context.Background(), stor, "recovery/job1.json"))
	exists, _ := stor.Exists(context.Background(), "recovery/job1.json")
	assert.False(t, exists)
}

// --- Recover: pending state ---

func TestRecover_PendingState(t *testing.T) {
	stor := testutil.NewMemStorage(nil)
	cat := &mockCatalog{meta: newMeta(0)}

	rm := RecoveryManifest{
		JobID:  "job-pending",
		State:  recoveryStatePending,
		Table:  "ns.tbl",
	}
	recoveryPath := "s3://bucket/ns/tbl/.firn/recovery/job-pending.json"
	require.NoError(t, writeRecovery(context.Background(), stor, "s3://bucket/ns/tbl/.firn/recovery/job-pending.json", rm))

	// recoverOne should delete the recovery file and not commit.
	err := recoverOne(context.Background(), stor, cat, tableID, recoveryPath)
	require.NoError(t, err)
	assert.Equal(t, 0, cat.commitCalls)
	exists, _ := stor.Exists(context.Background(), recoveryPath)
	assert.False(t, exists)
}

// --- Recover: uploaded state ---

func TestRecover_UploadedState(t *testing.T) {
	snap1 := int64(1)
	stor := testutil.NewMemStorage(nil)
	cat := &mockCatalog{meta: newMeta(1)}

	// Write a manifest file that ReadManifest can decode.
	entries := []iceberg.ManifestEntry{
		{Status: iceberg.EntryStatusAdded, SnapshotID: &snap1, DataFile: iceberg.DataFile{FilePath: "out.parquet", FileFormat: "PARQUET", FileSizeBytes: 512}},
	}
	require.NoError(t, iceberg.WriteManifest(context.Background(), stor, "meta/snap.avro", entries))

	rm := RecoveryManifest{
		JobID:            "job-uploaded",
		State:            recoveryStateUploaded,
		Table:            "ns.tbl",
		InputFiles:       []string{"a.parquet"},
		OutputFile:       "out.parquet",
		ManifestPath:     "meta/snap.avro",
		ManifestListPath: "meta/snap-ml.avro",
		ParentSnapshotID: 1,
		NewSnapshotID:    2,
	}
	recoveryPath := ".firn/recovery/job-uploaded.json"
	require.NoError(t, writeRecovery(context.Background(), stor, recoveryPath, rm))

	// Also write the input file so delete succeeds.
	require.NoError(t, stor.Write(context.Background(), "a.parquet", bytes.NewReader(nil), 0))

	err := recoverOne(context.Background(), stor, cat, tableID, recoveryPath)
	require.NoError(t, err)
	assert.Equal(t, 1, cat.commitCalls)

	// Input file and recovery manifest should both be gone.
	exists, _ := stor.Exists(context.Background(), recoveryPath)
	assert.False(t, exists)
	exists, _ = stor.Exists(context.Background(), "a.parquet")
	assert.False(t, exists)
}

// --- Recover: snapshot_committed state ---

func TestRecover_SnapshotCommittedState(t *testing.T) {
	stor := testutil.NewMemStorage(nil)
	cat := &mockCatalog{meta: newMeta(2)}

	// Write an input file to delete.
	require.NoError(t, stor.Write(context.Background(), "input.parquet", bytes.NewReader(nil), 0))

	rm := RecoveryManifest{
		JobID:      "job-committed",
		State:      recoveryStateSnapshotCommitted,
		InputFiles: []string{"input.parquet"},
	}
	recoveryPath := ".firn/recovery/job-committed.json"
	require.NoError(t, writeRecovery(context.Background(), stor, recoveryPath, rm))

	err := recoverOne(context.Background(), stor, cat, tableID, recoveryPath)
	require.NoError(t, err)
	assert.Equal(t, 0, cat.commitCalls) // No commit needed.

	exists, _ := stor.Exists(context.Background(), "input.parquet")
	assert.False(t, exists)
	exists, _ = stor.Exists(context.Background(), recoveryPath)
	assert.False(t, exists)
}

// --- Recover: conflict retry ---

func TestRecover_ConflictRetry(t *testing.T) {
	snap1 := int64(1)
	stor := testutil.NewMemStorage(nil)

	entries := []iceberg.ManifestEntry{
		{Status: iceberg.EntryStatusAdded, SnapshotID: &snap1, DataFile: iceberg.DataFile{FilePath: "out.parquet", FileFormat: "PARQUET"}},
	}
	require.NoError(t, iceberg.WriteManifest(context.Background(), stor, "meta/m.avro", entries))

	callCount := 0
	cat := &mockCatalog{meta: newMeta(1)}
	// Fail twice with conflict, succeed on third.
	realCommit := func() error {
		callCount++
		if callCount < 3 {
			return catalog.ErrConflict{Table: tableID}
		}
		return nil
	}

	rm := RecoveryManifest{
		JobID:            "job-conflict",
		State:            recoveryStateUploaded,
		Table:            "ns.tbl",
		ManifestPath:     "meta/m.avro",
		ManifestListPath: "meta/ml.avro",
		ParentSnapshotID: 1,
		NewSnapshotID:    2,
	}
	recoveryPath := ".firn/recovery/job-conflict.json"
	require.NoError(t, writeRecovery(context.Background(), stor, recoveryPath, rm))

	// Override commitErr to use the counter-based func.
	cat.commitErr = nil
	// Replace CommitTransaction with a counter-based version by wrapping.
	wrappedCat := &countingCatalog{inner: cat, commitFn: realCommit}

	err := recoverOne(context.Background(), stor, wrappedCat, tableID, recoveryPath)
	require.NoError(t, err)
	assert.Equal(t, 3, callCount)
}

type countingCatalog struct {
	inner    *mockCatalog
	commitFn func() error
}

func (c *countingCatalog) ListNamespaces(ctx context.Context) ([]string, error) {
	return c.inner.ListNamespaces(ctx)
}
func (c *countingCatalog) ListTables(ctx context.Context, ns string) ([]catalog.TableIdentifier, error) {
	return c.inner.ListTables(ctx, ns)
}
func (c *countingCatalog) LoadTable(ctx context.Context, id catalog.TableIdentifier) (*iceberg.TableMetadata, error) {
	return c.inner.LoadTable(ctx, id)
}
func (c *countingCatalog) CommitTransaction(_ context.Context, _ catalog.TableIdentifier, _ catalog.Transaction) error {
	err := c.commitFn()
	// On conflict, check if it's actually a catalog.ErrConflict
	var conflict catalog.ErrConflict
	if errors.As(err, &conflict) {
		return err
	}
	return err
}
