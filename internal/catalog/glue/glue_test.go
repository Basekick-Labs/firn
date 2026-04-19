package glue

import (
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/firn/internal/catalog"
	"github.com/basekick-labs/firn/internal/iceberg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- parseS3URI ---

func TestParseS3URI(t *testing.T) {
	tests := []struct {
		uri        string
		wantBucket string
		wantKey    string
		wantErr    bool
	}{
		{"s3://my-bucket/path/to/file.json", "my-bucket", "path/to/file.json", false},
		{"s3://bucket/metadata/v1.metadata.json", "bucket", "metadata/v1.metadata.json", false},
		{"s3://bucket/", "bucket", "", false},
		{"not-s3://bucket/key", "", "", true},
		{"s3://no-slash", "", "", true},
	}

	for _, tt := range tests {
		b, k, err := parseS3URI(tt.uri)
		if tt.wantErr {
			require.Error(t, err, "uri=%s", tt.uri)
		} else {
			require.NoError(t, err, "uri=%s", tt.uri)
			assert.Equal(t, tt.wantBucket, b, "uri=%s", tt.uri)
			assert.Equal(t, tt.wantKey, k, "uri=%s", tt.uri)
		}
	}
}

// --- isIcebergTable ---

func TestIsIcebergTable(t *testing.T) {
	assert.True(t, isIcebergTable(map[string]string{"table_type": "ICEBERG"}))
	assert.True(t, isIcebergTable(map[string]string{"table_type": "iceberg"}))
	assert.False(t, isIcebergTable(map[string]string{"table_type": "HIVE"}))
	assert.False(t, isIcebergTable(map[string]string{}))
	assert.False(t, isIcebergTable(nil))
}

// --- checkRequirement ---

func TestCheckRequirement_SnapshotIDMatch(t *testing.T) {
	meta := &iceberg.TableMetadata{CurrentSnapshotID: 42}
	req := catalog.Requirement{Type: "assert-current-snapshot-id", CurrentSnapshotID: 42}
	assert.NoError(t, checkRequirement(req, meta))
}

func TestCheckRequirement_SnapshotIDMismatch(t *testing.T) {
	meta := &iceberg.TableMetadata{CurrentSnapshotID: 42}
	req := catalog.Requirement{Type: "assert-current-snapshot-id", CurrentSnapshotID: 99}
	assert.Error(t, checkRequirement(req, meta))
}

func TestCheckRequirement_UnknownTypeReturnsError(t *testing.T) {
	meta := &iceberg.TableMetadata{}
	req := catalog.Requirement{Type: "assert-table-does-not-exist"}
	assert.Error(t, checkRequirement(req, meta))
}

// --- applyUpdates ---

func TestApplyUpdates_AddSnapshot(t *testing.T) {
	meta := &iceberg.TableMetadata{
		CurrentSnapshotID:  1,
		LastSequenceNumber: 0,
		Snapshots:          []iceberg.Snapshot{{SnapshotID: 1, TimestampMs: 1000}},
	}
	snap := iceberg.Snapshot{SnapshotID: 2, ParentSnapshotID: 1, SequenceNumber: 1, TimestampMs: 2000, ManifestList: "s3://b/ml.avro"}
	updates := []catalog.Update{
		{Type: "add-snapshot", Snapshot: &snap},
		{Type: "set-snapshot-ref", RefName: "main", SnapshotIDs: []int64{2}},
	}

	newMeta, err := applyUpdates(meta, updates)
	require.NoError(t, err)
	assert.Equal(t, int64(2), newMeta.CurrentSnapshotID)
	assert.Len(t, newMeta.Snapshots, 2)
	assert.Equal(t, int64(2), newMeta.Snapshots[1].SnapshotID)
	assert.Equal(t, int64(1), newMeta.LastSequenceNumber)
	// Original must not be mutated.
	assert.Equal(t, int64(1), meta.CurrentSnapshotID)
	assert.Len(t, meta.Snapshots, 1)
}

func TestApplyUpdates_RemoveSnapshots(t *testing.T) {
	meta := &iceberg.TableMetadata{
		CurrentSnapshotID: 3,
		Snapshots: []iceberg.Snapshot{
			{SnapshotID: 1},
			{SnapshotID: 2},
			{SnapshotID: 3},
		},
	}
	updates := []catalog.Update{
		{Type: "remove-snapshots", SnapshotIDs: []int64{1, 2}},
	}

	newMeta, err := applyUpdates(meta, updates)
	require.NoError(t, err)
	require.Len(t, newMeta.Snapshots, 1)
	assert.Equal(t, int64(3), newMeta.Snapshots[0].SnapshotID)
}

func TestApplyUpdates_SetSnapshotRef_NonMain(t *testing.T) {
	meta := &iceberg.TableMetadata{CurrentSnapshotID: 1}
	updates := []catalog.Update{
		{Type: "set-snapshot-ref", RefName: "branch-x", SnapshotIDs: []int64{99}},
	}
	newMeta, err := applyUpdates(meta, updates)
	require.NoError(t, err)
	// Non-main refs don't change CurrentSnapshotID.
	assert.Equal(t, int64(1), newMeta.CurrentSnapshotID)
}

func TestApplyUpdates_AddSnapshot_MissingSnapshot(t *testing.T) {
	meta := &iceberg.TableMetadata{}
	updates := []catalog.Update{{Type: "add-snapshot", Snapshot: nil}}
	_, err := applyUpdates(meta, updates)
	require.Error(t, err)
}

func TestApplyUpdates_LastUpdatedMs_Changes(t *testing.T) {
	meta := &iceberg.TableMetadata{LastUpdatedMs: 1000}
	before := time.Now().UnixMilli()
	newMeta, err := applyUpdates(meta, nil)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, newMeta.LastUpdatedMs, before)
}

// --- nextMetadataLocation ---

func TestNextMetadataLocation(t *testing.T) {
	loc, err := nextMetadataLocation("s3://bucket/ns/tbl/metadata/v2-abc.metadata.json")
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(loc, "s3://bucket/ns/tbl/metadata/"), "got: %s", loc)
	assert.True(t, strings.HasSuffix(loc, ".metadata.json"), "got: %s", loc)
	// Each call produces a unique path.
	loc2, err := nextMetadataLocation("s3://bucket/ns/tbl/metadata/v2-abc.metadata.json")
	require.NoError(t, err)
	assert.NotEqual(t, loc, loc2)
}

func TestNextMetadataLocation_NoSlash(t *testing.T) {
	_, err := nextMetadataLocation("noslash")
	require.Error(t, err)
}

// --- cloneParams ---

func TestCloneParams_IsIndependent(t *testing.T) {
	src := map[string]string{"a": "1", "b": "2"}
	dst := cloneParams(src)
	dst["a"] = "mutated"
	assert.Equal(t, "1", src["a"], "original should not be mutated")
}
