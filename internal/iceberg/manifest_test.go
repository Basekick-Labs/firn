package iceberg

import (
	"bytes"
	"context"
	"testing"

	"github.com/basekick-labs/firn/internal/testutil"
	"github.com/hamba/avro/v2/ocf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const manifestListSchemaJSON = `{
	"type": "record",
	"name": "manifest_file",
	"fields": [
		{"name": "manifest_path",        "type": "string"},
		{"name": "manifest_length",       "type": "long"},
		{"name": "partition_spec_id",     "type": "int"},
		{"name": "content",               "type": "int"},
		{"name": "sequence_number",       "type": "long"},
		{"name": "min_sequence_number",   "type": "long"},
		{"name": "added_snapshot_id",     "type": "long"},
		{"name": "added_files_count",     "type": "int"},
		{"name": "existing_files_count",  "type": "int"},
		{"name": "deleted_files_count",   "type": "int"},
		{"name": "added_rows_count",      "type": "long"},
		{"name": "existing_rows_count",   "type": "long"},
		{"name": "deleted_rows_count",    "type": "long"}
	]
}`

const manifestEntrySchemaJSON = `{
	"type": "record",
	"name": "manifest_entry",
	"fields": [
		{"name": "status",      "type": "int"},
		{"name": "snapshot_id", "type": ["null", "long"], "default": null},
		{"name": "data_file", "type": {
			"type": "record",
			"name": "r2",
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

func buildManifestListAvro(t *testing.T, entries []ManifestFile) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc, err := ocf.NewEncoder(manifestListSchemaJSON, &buf)
	require.NoError(t, err)
	for _, e := range entries {
		require.NoError(t, enc.Encode(e))
	}
	require.NoError(t, enc.Flush())
	return buf.Bytes()
}

func buildManifestAvro(t *testing.T, entries []ManifestEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc, err := ocf.NewEncoder(manifestEntrySchemaJSON, &buf)
	require.NoError(t, err)
	for _, e := range entries {
		require.NoError(t, enc.Encode(e))
	}
	require.NoError(t, enc.Flush())
	return buf.Bytes()
}

// --- ReadManifestList ---

func TestReadManifestList_ReturnsDataManifests(t *testing.T) {
	entries := []ManifestFile{
		{Path: "data/m1.avro", Content: ManifestContentData, AddedSnapshotID: 1},
		{Path: "deletes/m2.avro", Content: ManifestContentDeletes, AddedSnapshotID: 1},
		{Path: "data/m3.avro", Content: ManifestContentData, AddedSnapshotID: 2},
	}

	stor := testutil.NewMemStorage(map[string][]byte{
		"manifests/snap.avro": buildManifestListAvro(t, entries),
	})

	result, err := ReadManifestList(context.Background(), stor, "s3://bucket/manifests/snap.avro")
	require.NoError(t, err)
	assert.Len(t, result, 2)
	assert.Equal(t, "data/m1.avro", result[0].Path)
	assert.Equal(t, "data/m3.avro", result[1].Path)
}

func TestReadManifestList_Empty(t *testing.T) {
	stor := testutil.NewMemStorage(map[string][]byte{
		"snap.avro": buildManifestListAvro(t, nil),
	})

	result, err := ReadManifestList(context.Background(), stor, "snap.avro")
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestReadManifestList_StorageError(t *testing.T) {
	stor := testutil.NewMemStorage(nil)
	_, err := ReadManifestList(context.Background(), stor, "missing.avro")
	require.Error(t, err)
}

// --- ReadManifest ---

func TestReadManifest_FiltersDeletedEntries(t *testing.T) {
	snap1 := int64(1)
	entries := []ManifestEntry{
		{Status: EntryStatusExisting, SnapshotID: &snap1, DataFile: DataFile{FilePath: "data/a.parquet", FileSizeBytes: 100}},
		{Status: EntryStatusAdded, SnapshotID: &snap1, DataFile: DataFile{FilePath: "data/b.parquet", FileSizeBytes: 200}},
		{Status: EntryStatusDeleted, SnapshotID: &snap1, DataFile: DataFile{FilePath: "data/c.parquet", FileSizeBytes: 300}},
	}

	stor := testutil.NewMemStorage(map[string][]byte{
		"m1.avro": buildManifestAvro(t, entries),
	})

	result, err := ReadManifest(context.Background(), stor, "m1.avro")
	require.NoError(t, err)
	assert.Len(t, result, 2)
	assert.Equal(t, "data/a.parquet", result[0].DataFile.FilePath)
	assert.Equal(t, "data/b.parquet", result[1].DataFile.FilePath)
}

func TestReadManifest_Empty(t *testing.T) {
	stor := testutil.NewMemStorage(map[string][]byte{
		"empty.avro": buildManifestAvro(t, nil),
	})

	result, err := ReadManifest(context.Background(), stor, "empty.avro")
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestReadManifest_NullSnapshotID(t *testing.T) {
	entries := []ManifestEntry{
		{Status: EntryStatusExisting, SnapshotID: nil, DataFile: DataFile{FilePath: "data/x.parquet", FileSizeBytes: 512}},
	}

	stor := testutil.NewMemStorage(map[string][]byte{
		"m.avro": buildManifestAvro(t, entries),
	})

	result, err := ReadManifest(context.Background(), stor, "m.avro")
	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Nil(t, result[0].SnapshotID)
}

// --- uriToPath ---

func TestURIToPath(t *testing.T) {
	tests := []struct {
		uri     string
		want    string
		wantErr bool
	}{
		{"s3://my-bucket/path/to/file.avro", "path/to/file.avro", false},
		{"s3://my-bucket/manifests/snap-42.avro", "manifests/snap-42.avro", false},
		{"path/to/file.avro", "path/to/file.avro", false},
		{"/absolute/path.avro", "/absolute/path.avro", false},
		{"s3://my-bucket/", "", true},
		{"s3://my-bucket", "", true},
	}
	for _, tt := range tests {
		got, err := uriToPath(tt.uri)
		if tt.wantErr {
			require.Error(t, err, "uri: %s", tt.uri)
		} else {
			require.NoError(t, err, "uri: %s", tt.uri)
			assert.Equal(t, tt.want, got, "uri: %s", tt.uri)
		}
	}
}
