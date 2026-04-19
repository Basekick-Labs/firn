package compaction

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

// --- tests ---

func TestFindCandidates_EmptyTable(t *testing.T) {
	engine := NewEngine(&mockCatalog{meta: &iceberg.TableMetadata{}}, testutil.NewMemStorage(nil), &config.Config{})
	candidates, err := engine.FindCandidates(context.Background(),
		catalog.TableIdentifier{Namespace: "ns", Name: "tbl"},
		config.CompactionPolicy{Enabled: true, MinFileCount: 2, MinFileAgeMinutes: 0},
	)
	require.NoError(t, err)
	assert.Empty(t, candidates)
}

func TestFindCandidates_TwoPartitionsOneEligible(t *testing.T) {
	snap1 := int64(1)
	snapTime := time.Now().UTC().Add(-2 * time.Hour)

	manifestListData := encodeAvro(t, manifestListSchemaJSON, []iceberg.ManifestFile{
		{Path: "manifests/m1.avro", Content: iceberg.ManifestContentData},
		{Path: "manifests/m2.avro", Content: iceberg.ManifestContentData},
	})

	// partition A: 3 old files → eligible
	m1Data := encodeAvro(t, manifestEntrySchemaJSON, []iceberg.ManifestEntry{
		{Status: iceberg.EntryStatusAdded, SnapshotID: &snap1, DataFile: iceberg.DataFile{FilePath: "data/partA/f1.parquet", FileSizeBytes: 100}},
		{Status: iceberg.EntryStatusAdded, SnapshotID: &snap1, DataFile: iceberg.DataFile{FilePath: "data/partA/f2.parquet", FileSizeBytes: 100}},
		{Status: iceberg.EntryStatusAdded, SnapshotID: &snap1, DataFile: iceberg.DataFile{FilePath: "data/partA/f3.parquet", FileSizeBytes: 100}},
	})

	// partition B: 1 file → not eligible (below MinFileCount)
	m2Data := encodeAvro(t, manifestEntrySchemaJSON, []iceberg.ManifestEntry{
		{Status: iceberg.EntryStatusAdded, SnapshotID: &snap1, DataFile: iceberg.DataFile{FilePath: "data/partB/f1.parquet", FileSizeBytes: 100}},
	})

	stor := testutil.NewMemStorage(map[string][]byte{
		"manifests/snap.avro": manifestListData,
		"manifests/m1.avro":   m1Data,
		"manifests/m2.avro":   m2Data,
	})

	meta := &iceberg.TableMetadata{
		CurrentSnapshotID: 1,
		Snapshots: []iceberg.Snapshot{
			{SnapshotID: 1, TimestampMs: snapTime.UnixMilli(), ManifestList: "manifests/snap.avro"},
		},
	}

	engine := NewEngine(&mockCatalog{meta: meta}, stor, &config.Config{})
	candidates, err := engine.FindCandidates(context.Background(),
		catalog.TableIdentifier{Namespace: "ns", Name: "tbl"},
		config.CompactionPolicy{Enabled: true, MinFileCount: 2, MinFileAgeMinutes: 60},
	)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	assert.Equal(t, "data/partA", candidates[0].Partition)
	assert.Len(t, candidates[0].Files, 3)
}

func TestFindCandidates_AgeFilter(t *testing.T) {
	snap1 := int64(1)
	snapTime := time.Now().UTC().Add(-10 * time.Minute)

	manifestListData := encodeAvro(t, manifestListSchemaJSON, []iceberg.ManifestFile{
		{Path: "manifests/m1.avro", Content: iceberg.ManifestContentData},
	})
	m1Data := encodeAvro(t, manifestEntrySchemaJSON, []iceberg.ManifestEntry{
		{Status: iceberg.EntryStatusAdded, SnapshotID: &snap1, DataFile: iceberg.DataFile{FilePath: "data/p/f1.parquet", FileSizeBytes: 100}},
		{Status: iceberg.EntryStatusAdded, SnapshotID: &snap1, DataFile: iceberg.DataFile{FilePath: "data/p/f2.parquet", FileSizeBytes: 100}},
		{Status: iceberg.EntryStatusAdded, SnapshotID: &snap1, DataFile: iceberg.DataFile{FilePath: "data/p/f3.parquet", FileSizeBytes: 100}},
	})

	stor := testutil.NewMemStorage(map[string][]byte{
		"manifests/snap.avro": manifestListData,
		"manifests/m1.avro":   m1Data,
	})

	meta := &iceberg.TableMetadata{
		CurrentSnapshotID: 1,
		Snapshots: []iceberg.Snapshot{
			{SnapshotID: 1, TimestampMs: snapTime.UnixMilli(), ManifestList: "manifests/snap.avro"},
		},
	}

	engine := NewEngine(&mockCatalog{meta: meta}, stor, &config.Config{})
	candidates, err := engine.FindCandidates(context.Background(),
		catalog.TableIdentifier{Namespace: "ns", Name: "tbl"},
		config.CompactionPolicy{Enabled: true, MinFileCount: 2, MinFileAgeMinutes: 60},
	)
	require.NoError(t, err)
	assert.Empty(t, candidates, "files too new should not be candidates")
}

func TestFindCandidates_DisabledPolicy(t *testing.T) {
	engine := NewEngine(&mockCatalog{meta: &iceberg.TableMetadata{}}, testutil.NewMemStorage(nil), &config.Config{})
	candidates, err := engine.FindCandidates(context.Background(),
		catalog.TableIdentifier{Namespace: "ns", Name: "tbl"},
		config.CompactionPolicy{Enabled: false},
	)
	require.NoError(t, err)
	assert.Empty(t, candidates)
}

func TestFindCandidates_SortStrategyRequiresSortKeys(t *testing.T) {
	// panicCatalog panics if LoadTable is called, enforcing that validation
	// short-circuits before any catalog I/O.
	engine := NewEngine(&panicCatalog{}, testutil.NewMemStorage(nil), &config.Config{})
	_, err := engine.FindCandidates(context.Background(),
		catalog.TableIdentifier{Namespace: "ns", Name: "tbl"},
		config.CompactionPolicy{Enabled: true, Strategy: "sort", SortKeys: nil},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sort_key")
}

func TestBuildOrderClause(t *testing.T) {
	tests := []struct {
		sortKeys []string
		want     string
		wantErr  bool
	}{
		{nil, "", false},
		{[]string{}, "", false},
		{[]string{"ts"}, `ORDER BY "ts"`, false},
		{[]string{"ts", "user_id"}, `ORDER BY "ts", "user_id"`, false},
		{[]string{"ts DESC"}, "", true},  // space not allowed
		{[]string{"1bad"}, "", true},     // must start with letter or underscore
		{[]string{"ts; DROP TABLE x"}, "", true},
	}
	for _, tc := range tests {
		got, err := buildOrderClause(tc.sortKeys)
		if tc.wantErr {
			assert.Error(t, err)
		} else {
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		}
	}
}

func TestFindCandidates_DeleteManifestSkipped(t *testing.T) {
	snapTime := time.Now().UTC().Add(-2 * time.Hour)

	manifestListData := encodeAvro(t, manifestListSchemaJSON, []iceberg.ManifestFile{
		{Path: "manifests/deletes.avro", Content: iceberg.ManifestContentDeletes},
	})

	stor := testutil.NewMemStorage(map[string][]byte{
		"manifests/snap.avro": manifestListData,
	})

	meta := &iceberg.TableMetadata{
		CurrentSnapshotID: 1,
		Snapshots: []iceberg.Snapshot{
			{SnapshotID: 1, TimestampMs: snapTime.UnixMilli(), ManifestList: "manifests/snap.avro"},
		},
	}

	engine := NewEngine(&mockCatalog{meta: meta}, stor, &config.Config{})
	candidates, err := engine.FindCandidates(context.Background(),
		catalog.TableIdentifier{Namespace: "ns", Name: "tbl"},
		config.CompactionPolicy{Enabled: true, MinFileCount: 1, MinFileAgeMinutes: 0},
	)
	require.NoError(t, err)
	assert.Empty(t, candidates, "delete manifests should be skipped")
}
