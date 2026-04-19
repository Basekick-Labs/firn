package orphan

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/basekick-labs/firn/internal/catalog"
	"github.com/basekick-labs/firn/internal/config"
	"github.com/basekick-labs/firn/internal/iceberg"
	"github.com/basekick-labs/firn/internal/testutil"
	"github.com/hamba/avro/v2/ocf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- avro builders ---

const manifestListSchemaJSON = `{
	"type": "record", "name": "manifest_file",
	"fields": [
		{"name": "manifest_path",       "type": "string"},
		{"name": "manifest_length",      "type": "long"},
		{"name": "partition_spec_id",    "type": "int"},
		{"name": "content",              "type": "int"},
		{"name": "sequence_number",      "type": "long"},
		{"name": "min_sequence_number",  "type": "long"},
		{"name": "added_snapshot_id",    "type": "long"},
		{"name": "added_files_count",    "type": "int"},
		{"name": "existing_files_count", "type": "int"},
		{"name": "deleted_files_count",  "type": "int"},
		{"name": "added_rows_count",     "type": "long"},
		{"name": "existing_rows_count",  "type": "long"},
		{"name": "deleted_rows_count",   "type": "long"}
	]
}`

const manifestEntrySchemaJSON = `{
	"type": "record", "name": "manifest_entry",
	"fields": [
		{"name": "status",      "type": "int"},
		{"name": "snapshot_id", "type": ["null","long"], "default": null},
		{"name": "data_file", "type": {
			"type": "record", "name": "r2",
			"fields": [
				{"name": "content",            "type": "int"},
				{"name": "file_path",          "type": "string"},
				{"name": "file_format",        "type": "string"},
				{"name": "record_count",       "type": "long"},
				{"name": "file_size_in_bytes", "type": "long"}
			]
		}}
	]
}`

func encodeAvro(t *testing.T, schema string, records any) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc, err := ocf.NewEncoder(schema, &buf)
	require.NoError(t, err)
	switch v := records.(type) {
	case []iceberg.ManifestFile:
		for _, r := range v {
			require.NoError(t, enc.Encode(r))
		}
	case []iceberg.ManifestEntry:
		for _, r := range v {
			require.NoError(t, enc.Encode(r))
		}
	}
	require.NoError(t, enc.Flush())
	return buf.Bytes()
}

// --- mock catalog ---

type mockCatalog struct {
	meta *iceberg.TableMetadata
}

func (m *mockCatalog) ListNamespaces(_ context.Context) ([]string, error) { return nil, nil }
func (m *mockCatalog) ListTables(_ context.Context, _ string) ([]catalog.TableIdentifier, error) {
	return nil, nil
}
func (m *mockCatalog) LoadTable(_ context.Context, _ catalog.TableIdentifier) (*iceberg.TableMetadata, error) {
	return m.meta, nil
}
func (m *mockCatalog) CommitTransaction(_ context.Context, _ catalog.TableIdentifier, _ catalog.Transaction) error {
	return nil
}

var tableID = catalog.TableIdentifier{Namespace: "ns", Name: "t"}

func policy(hours int) config.OrphanCleanupPolicy {
	return config.OrphanCleanupPolicy{Enabled: true, GracePeriodHours: hours}
}

// --- helpers ---

// buildTable creates a simple single-snapshot table with one manifest list,
// one manifest, and one data file. Returns the MemStorage pre-populated with
// all Avro files and the TableMetadata.
func buildTable(t *testing.T, tableLocation string) (*testutil.MemStorage, *iceberg.TableMetadata) {
	t.Helper()

	mlKey := "table/metadata/snap-1-ml.avro"
	mKey := "table/metadata/snap-1-manifest.avro"
	dataKey := "table/data/part-00000.parquet"

	mURI := "s3://bucket/" + mKey
	dataURI := "s3://bucket/" + dataKey

	stor := testutil.NewMemStorage(map[string][]byte{
		mlKey: encodeAvro(t, manifestListSchemaJSON, []iceberg.ManifestFile{
			{Path: mURI, AddedSnapshotID: 1},
		}),
		mKey: encodeAvro(t, manifestEntrySchemaJSON, []iceberg.ManifestEntry{
			{Status: iceberg.EntryStatusAdded, DataFile: iceberg.DataFile{FilePath: dataURI, FileFormat: "PARQUET"}},
		}),
		dataKey: []byte("parquet-data"),
	})

	meta := &iceberg.TableMetadata{
		Location:          tableLocation,
		CurrentSnapshotID: 1,
		Snapshots: []iceberg.Snapshot{
			{SnapshotID: 1, ManifestList: "s3://bucket/" + mlKey},
		},
	}
	return stor, meta
}

// --- tests ---

func TestExecuteCleanup_Disabled(t *testing.T) {
	cat := &mockCatalog{meta: &iceberg.TableMetadata{Location: "s3://bucket/table"}}
	e := NewEngine(cat, testutil.NewMemStorage(nil))
	result, err := e.ExecuteCleanup(context.Background(), tableID, config.OrphanCleanupPolicy{Enabled: false})
	require.NoError(t, err)
	assert.Equal(t, 0, result.ScannedFiles)
	assert.Equal(t, 0, result.DeletedFiles)
}

func TestExecuteCleanup_NoOrphans(t *testing.T) {
	stor, meta := buildTable(t, "s3://bucket/table")
	e := NewEngine(&mockCatalog{meta: meta}, stor)
	result, err := e.ExecuteCleanup(context.Background(), tableID, policy(24))
	require.NoError(t, err)
	assert.Equal(t, 3, result.ScannedFiles) // ml + manifest + data
	assert.Equal(t, 0, result.DeletedFiles)
}

func TestExecuteCleanup_DeletesOrphan(t *testing.T) {
	stor, meta := buildTable(t, "s3://bucket/table")

	// Add an orphan file older than the grace period (seed files are 48h old by default).
	orphanKey := "table/data/orphan-old.parquet"
	stor.WriteOld(orphanKey, []byte("orphan"), 48*time.Hour+time.Minute)

	e := NewEngine(&mockCatalog{meta: meta}, stor)
	result, err := e.ExecuteCleanup(context.Background(), tableID, policy(24))
	require.NoError(t, err)
	assert.Equal(t, 1, result.DeletedFiles)

	exists, _ := stor.Exists(context.Background(), orphanKey)
	assert.False(t, exists, "orphan file should be deleted")
}

func TestExecuteCleanup_GracePeriodSkips(t *testing.T) {
	stor, meta := buildTable(t, "s3://bucket/table")

	// Add a recent orphan (1 hour old) — should NOT be deleted under a 24h grace period.
	orphanKey := "table/data/orphan-new.parquet"
	stor.WriteOld(orphanKey, []byte("orphan"), time.Hour)

	e := NewEngine(&mockCatalog{meta: meta}, stor)
	result, err := e.ExecuteCleanup(context.Background(), tableID, policy(24))
	require.NoError(t, err)
	assert.Equal(t, 0, result.DeletedFiles)
	assert.Equal(t, 1, result.SkippedFiles)

	exists, _ := stor.Exists(context.Background(), orphanKey)
	assert.True(t, exists, "recent orphan must be preserved")
}

func TestExecuteCleanup_NeverDeletesJSON(t *testing.T) {
	stor, meta := buildTable(t, "s3://bucket/table")

	// Add an old JSON metadata file that is NOT in the live set.
	jsonKey := "table/metadata/v2.metadata.json"
	stor.WriteOld(jsonKey, []byte(`{}`), 200*time.Hour)

	e := NewEngine(&mockCatalog{meta: meta}, stor)
	result, err := e.ExecuteCleanup(context.Background(), tableID, policy(24))
	require.NoError(t, err)
	assert.Equal(t, 0, result.DeletedFiles)

	exists, _ := stor.Exists(context.Background(), jsonKey)
	assert.True(t, exists, "JSON metadata file must never be deleted")
}

func TestExecuteCleanup_LiveSetError(t *testing.T) {
	// Table references a manifest list that doesn't exist in storage.
	stor := testutil.NewMemStorage(nil)
	meta := &iceberg.TableMetadata{
		Location:          "s3://bucket/table",
		CurrentSnapshotID: 1,
		Snapshots: []iceberg.Snapshot{
			{SnapshotID: 1, ManifestList: "s3://bucket/table/metadata/missing-ml.avro"},
		},
	}

	// Add an orphan that should NOT be deleted because GC is aborted.
	orphanKey := "table/data/orphan.parquet"
	stor.WriteOld(orphanKey, []byte("data"), 200*time.Hour)

	e := NewEngine(&mockCatalog{meta: meta}, stor)
	_, err := e.ExecuteCleanup(context.Background(), tableID, policy(24))
	require.Error(t, err, "should return error when manifest list cannot be read")

	exists, _ := stor.Exists(context.Background(), orphanKey)
	assert.True(t, exists, "no files should be deleted when live set collection fails")
}

// TestExecuteCleanup_NeverDeletesDeleteManifest verifies that Iceberg v2 delete
// manifest files (positional / equality deletes, ManifestContentDeletes == 1)
// are included in the live-file set and never treated as orphans.
func TestExecuteCleanup_NeverDeletesDeleteManifest(t *testing.T) {
	dataMLKey := "table/metadata/snap-1-ml.avro"
	dataMKey := "table/metadata/snap-1-data-manifest.avro"
	deleteMLKey := "table/metadata/snap-2-ml.avro"
	deleteMKey := "table/metadata/snap-2-delete-manifest.avro"
	dataKey := "table/data/part-00000.parquet"
	deleteFKey := "table/data/part-00000-delete.parquet"

	mURI := "s3://bucket/" + dataMKey
	dataURI := "s3://bucket/" + dataKey
	deleteMURI := "s3://bucket/" + deleteMKey
	deleteFURI := "s3://bucket/" + deleteFKey

	stor := testutil.NewMemStorage(map[string][]byte{
		// Snapshot 1: data manifest list with one data manifest.
		dataMLKey: encodeAvro(t, manifestListSchemaJSON, []iceberg.ManifestFile{
			{Path: mURI, Content: iceberg.ManifestContentData, AddedSnapshotID: 1},
		}),
		dataMKey: encodeAvro(t, manifestEntrySchemaJSON, []iceberg.ManifestEntry{
			{Status: iceberg.EntryStatusAdded, DataFile: iceberg.DataFile{FilePath: dataURI, FileFormat: "PARQUET"}},
		}),
		// Snapshot 2: manifest list that contains a delete manifest (content=1).
		deleteMLKey: encodeAvro(t, manifestListSchemaJSON, []iceberg.ManifestFile{
			{Path: mURI, Content: iceberg.ManifestContentData, AddedSnapshotID: 1},
			{Path: deleteMURI, Content: iceberg.ManifestContentDeletes, AddedSnapshotID: 2},
		}),
		deleteMKey: encodeAvro(t, manifestEntrySchemaJSON, []iceberg.ManifestEntry{
			{Status: iceberg.EntryStatusAdded, DataFile: iceberg.DataFile{FilePath: deleteFURI, FileFormat: "PARQUET"}},
		}),
		dataKey:    []byte("parquet-data"),
		deleteFKey: []byte("parquet-delete-data"),
	})

	meta := &iceberg.TableMetadata{
		Location:          "s3://bucket/table",
		CurrentSnapshotID: 2,
		Snapshots: []iceberg.Snapshot{
			{SnapshotID: 1, ManifestList: "s3://bucket/" + dataMLKey},
			{SnapshotID: 2, ManifestList: "s3://bucket/" + deleteMLKey},
		},
	}

	e := NewEngine(&mockCatalog{meta: meta}, stor)
	result, err := e.ExecuteCleanup(context.Background(), tableID, policy(24))
	require.NoError(t, err)
	assert.Equal(t, 0, result.DeletedFiles, "no files should be deleted")

	exists, _ := stor.Exists(context.Background(), deleteMKey)
	assert.True(t, exists, "delete manifest file must be kept as live")
	exists, _ = stor.Exists(context.Background(), deleteFKey)
	assert.True(t, exists, "positional delete file must be kept as live")
}

func TestCollectLiveFiles(t *testing.T) {
	mlKey1 := "table/metadata/snap-1-ml.avro"
	mlKey2 := "table/metadata/snap-2-ml.avro"
	mKey1 := "table/metadata/snap-1-manifest.avro"
	mKey2 := "table/metadata/snap-2-manifest.avro"
	dataKey1 := "table/data/part-00001.parquet"
	dataKey2 := "table/data/part-00002.parquet"

	stor := testutil.NewMemStorage(map[string][]byte{
		mlKey1: encodeAvro(t, manifestListSchemaJSON, []iceberg.ManifestFile{
			{Path: "s3://bucket/" + mKey1, AddedSnapshotID: 1},
		}),
		mlKey2: encodeAvro(t, manifestListSchemaJSON, []iceberg.ManifestFile{
			{Path: "s3://bucket/" + mKey2, AddedSnapshotID: 2},
		}),
		mKey1: encodeAvro(t, manifestEntrySchemaJSON, []iceberg.ManifestEntry{
			{Status: iceberg.EntryStatusAdded, DataFile: iceberg.DataFile{FilePath: "s3://bucket/" + dataKey1, FileFormat: "PARQUET"}},
		}),
		mKey2: encodeAvro(t, manifestEntrySchemaJSON, []iceberg.ManifestEntry{
			{Status: iceberg.EntryStatusAdded, DataFile: iceberg.DataFile{FilePath: "s3://bucket/" + dataKey2, FileFormat: "PARQUET"}},
		}),
	})

	meta := &iceberg.TableMetadata{
		Location:          "s3://bucket/table",
		CurrentSnapshotID: 2,
		Snapshots: []iceberg.Snapshot{
			{SnapshotID: 1, ManifestList: "s3://bucket/" + mlKey1},
			{SnapshotID: 2, ManifestList: "s3://bucket/" + mlKey2},
		},
	}

	e := NewEngine(&mockCatalog{meta: meta}, stor)
	live, err := e.collectLiveFiles(context.Background(), meta)
	require.NoError(t, err)

	assert.True(t, live[mlKey1], "manifest list 1 must be live")
	assert.True(t, live[mlKey2], "manifest list 2 must be live")
	assert.True(t, live[mKey1], "manifest 1 must be live")
	assert.True(t, live[mKey2], "manifest 2 must be live")
	assert.True(t, live[dataKey1], "data file 1 must be live")
	assert.True(t, live[dataKey2], "data file 2 must be live")
}
