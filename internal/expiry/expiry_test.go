package expiry

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/basekick-labs/firn/internal/catalog"
	"github.com/basekick-labs/firn/internal/config"
	"github.com/basekick-labs/firn/internal/iceberg"
	"github.com/basekick-labs/firn/internal/retry"
	"github.com/basekick-labs/firn/internal/testutil"
	"github.com/hamba/avro/v2/ocf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- avro builders (mirrored from compaction tests) ---

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
	meta        *iceberg.TableMetadata
	commitErr   error
	commitCalls int
	loadCalls   int
}

func (m *mockCatalog) ListNamespaces(_ context.Context) ([]string, error) { return nil, nil }
func (m *mockCatalog) ListTables(_ context.Context, _ string) ([]catalog.TableIdentifier, error) {
	return nil, nil
}
func (m *mockCatalog) LoadTable(_ context.Context, _ catalog.TableIdentifier) (*iceberg.TableMetadata, error) {
	m.loadCalls++
	return m.meta, nil
}
func (m *mockCatalog) CommitTransaction(_ context.Context, _ catalog.TableIdentifier, _ catalog.Transaction) error {
	m.commitCalls++
	return m.commitErr
}

// --- helpers ---

func ms(t time.Time) int64 { return t.UnixMilli() }

var tableID = catalog.TableIdentifier{Namespace: "ns", Name: "t"}

// --- SelectExpired tests ---

func TestSelectExpired(t *testing.T) {
	now := time.Date(2025, 1, 10, 12, 0, 0, 0, time.UTC)
	old := now.Add(-200 * time.Hour) // well past any MaxSnapshotAgeHours
	fresh := now.Add(-1 * time.Hour) // within any reasonable window

	policy := config.SnapshotExpiry{
		Enabled:             testutil.BoolPtr(true),
		MinSnapshotsToKeep:  2,
		MaxSnapshotAgeHours: 120,
	}

	tests := []struct {
		name     string
		meta     *iceberg.TableMetadata
		policy   config.SnapshotExpiry
		wantIDs  []int64
	}{
		{
			name:    "disabled policy",
			meta:    &iceberg.TableMetadata{Snapshots: []iceberg.Snapshot{{SnapshotID: 1, TimestampMs: ms(old)}}},
			policy:  config.SnapshotExpiry{Enabled: testutil.BoolPtr(false)},
			wantIDs: nil,
		},
		{
			name:    "empty snapshots",
			meta:    &iceberg.TableMetadata{},
			policy:  policy,
			wantIDs: nil,
		},
		{
			name: "single snapshot is current (ancestor)",
			meta: &iceberg.TableMetadata{
				CurrentSnapshotID: 1,
				Snapshots:         []iceberg.Snapshot{{SnapshotID: 1, TimestampMs: ms(old)}},
			},
			policy:  policy,
			wantIDs: nil,
		},
		{
			name: "all snapshots within age window",
			meta: &iceberg.TableMetadata{
				CurrentSnapshotID: 3,
				Snapshots: []iceberg.Snapshot{
					{SnapshotID: 1, ParentSnapshotID: 0, TimestampMs: ms(fresh)},
					{SnapshotID: 2, ParentSnapshotID: 1, TimestampMs: ms(fresh)},
					{SnapshotID: 3, ParentSnapshotID: 2, TimestampMs: ms(fresh)},
				},
			},
			policy:  policy,
			wantIDs: nil,
		},
		{
			name: "two expired non-ancestors, one current",
			meta: &iceberg.TableMetadata{
				CurrentSnapshotID: 3,
				Snapshots: []iceberg.Snapshot{
					// 1 and 2 are old side branches — not ancestors of 3
					{SnapshotID: 1, ParentSnapshotID: 0, TimestampMs: ms(old)},
					{SnapshotID: 2, ParentSnapshotID: 0, TimestampMs: ms(old)},
					{SnapshotID: 3, ParentSnapshotID: 0, TimestampMs: ms(fresh)},
				},
			},
			policy:  config.SnapshotExpiry{Enabled: testutil.BoolPtr(true), MinSnapshotsToKeep: 1, MaxSnapshotAgeHours: 120},
			wantIDs: []int64{1, 2},
		},
		{
			name: "MinSnapshotsToKeep floor clips expiry",
			// 6 total, 4 old non-ancestors eligible, MinKeep=5 → must keep 5
			// total kept without floor = 6-4 = 2, need 3 more kept → trim 3 from expirables → only 1 expires
			meta: &iceberg.TableMetadata{
				CurrentSnapshotID: 6,
				Snapshots: []iceberg.Snapshot{
					{SnapshotID: 1, ParentSnapshotID: 0, TimestampMs: ms(old)},
					{SnapshotID: 2, ParentSnapshotID: 0, TimestampMs: old.Add(time.Hour).UnixMilli()},
					{SnapshotID: 3, ParentSnapshotID: 0, TimestampMs: old.Add(2 * time.Hour).UnixMilli()},
					{SnapshotID: 4, ParentSnapshotID: 0, TimestampMs: old.Add(3 * time.Hour).UnixMilli()},
					{SnapshotID: 5, ParentSnapshotID: 0, TimestampMs: ms(fresh)},
					{SnapshotID: 6, ParentSnapshotID: 5, TimestampMs: ms(fresh)},
				},
			},
			policy:  config.SnapshotExpiry{Enabled: testutil.BoolPtr(true), MinSnapshotsToKeep: 5, MaxSnapshotAgeHours: 120},
			wantIDs: []int64{4}, // oldest 3 of the 4 expirables are kept; newest expirable (4) is dropped
		},
		{
			name: "full ancestry chain all old",
			meta: &iceberg.TableMetadata{
				CurrentSnapshotID: 3,
				Snapshots: []iceberg.Snapshot{
					{SnapshotID: 1, ParentSnapshotID: 0, TimestampMs: ms(old)},
					{SnapshotID: 2, ParentSnapshotID: 1, TimestampMs: ms(old)},
					{SnapshotID: 3, ParentSnapshotID: 2, TimestampMs: ms(old)},
				},
			},
			policy:  policy,
			wantIDs: nil,
		},
		{
			name: "non-ancestor alongside protected chain",
			meta: &iceberg.TableMetadata{
				CurrentSnapshotID: 3,
				Snapshots: []iceberg.Snapshot{
					{SnapshotID: 1, ParentSnapshotID: 0, TimestampMs: ms(old)}, // ancestor of 3
					{SnapshotID: 2, ParentSnapshotID: 0, TimestampMs: ms(old)}, // NOT ancestor of 3
					{SnapshotID: 3, ParentSnapshotID: 1, TimestampMs: ms(fresh)},
				},
			},
			policy:  config.SnapshotExpiry{Enabled: testutil.BoolPtr(true), MinSnapshotsToKeep: 1, MaxSnapshotAgeHours: 120},
			wantIDs: []int64{2},
		},
		{
			name: "MinSnapshotsToKeep larger than total snapshots",
			meta: &iceberg.TableMetadata{
				CurrentSnapshotID: 2,
				Snapshots: []iceberg.Snapshot{
					{SnapshotID: 1, ParentSnapshotID: 0, TimestampMs: ms(old)},
					{SnapshotID: 2, ParentSnapshotID: 0, TimestampMs: ms(fresh)},
				},
			},
			policy:  config.SnapshotExpiry{Enabled: testutil.BoolPtr(true), MinSnapshotsToKeep: 10, MaxSnapshotAgeHours: 120},
			wantIDs: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := SelectExpired(tc.meta, tc.policy, now)
			assert.ElementsMatch(t, tc.wantIDs, got)
		})
	}
}

// --- ExecuteExpiry integration tests ---

func policy120() config.SnapshotExpiry {
	return config.SnapshotExpiry{Enabled: testutil.BoolPtr(true), MinSnapshotsToKeep: 1, MaxSnapshotAgeHours: 120}
}

func TestExecuteExpiry_DisabledPolicy(t *testing.T) {
	cat := &mockCatalog{meta: &iceberg.TableMetadata{
		CurrentSnapshotID: 1,
		Snapshots:         []iceberg.Snapshot{{SnapshotID: 1, TimestampMs: ms(time.Now().Add(-1000 * time.Hour))}},
	}}
	e := NewEngine(cat, testutil.NewMemStorage(nil), retry.New(retry.Config{MaxAttempts: 3}))
	result, err := e.ExecuteExpiry(context.Background(), tableID, config.SnapshotExpiry{Enabled: testutil.BoolPtr(false)})
	require.NoError(t, err)
	assert.Equal(t, 0, result.ExpiredSnapshots)
	assert.Equal(t, 0, cat.commitCalls)
}

func TestExecuteExpiry_NoExpiredSnapshots(t *testing.T) {
	now := time.Now()
	cat := &mockCatalog{meta: &iceberg.TableMetadata{
		CurrentSnapshotID: 1,
		Snapshots:         []iceberg.Snapshot{{SnapshotID: 1, TimestampMs: ms(now)}},
	}}
	e := NewEngine(cat, testutil.NewMemStorage(nil), retry.New(retry.Config{MaxAttempts: 3}))
	result, err := e.ExecuteExpiry(context.Background(), tableID, policy120())
	require.NoError(t, err)
	assert.Equal(t, 0, result.ExpiredSnapshots)
	assert.Equal(t, 0, cat.commitCalls)
}

func TestExecuteExpiry_ExpiresSnapshots(t *testing.T) {
	now := time.Now()
	old := now.Add(-200 * time.Hour)

	// Snapshot 1 is old and not an ancestor of current (2).
	// Snapshot 2 is current.
	mlPath1 := "table/metadata/snap-1-ml.avro"
	mPath1 := "s3://bucket/table/metadata/snap-1-manifest.avro"
	dataPath1 := "s3://bucket/table/data/snap-1-data.parquet"

	mlPath2 := "table/metadata/snap-2-ml.avro"

	stor := testutil.NewMemStorage(map[string][]byte{
		mlPath1: encodeAvro(t, manifestListSchemaJSON, []iceberg.ManifestFile{
			{Path: mPath1, AddedSnapshotID: 1},
		}),
		"table/metadata/snap-1-manifest.avro": encodeAvro(t, manifestEntrySchemaJSON, []iceberg.ManifestEntry{
			{Status: iceberg.EntryStatusAdded, DataFile: iceberg.DataFile{FilePath: dataPath1, FileFormat: "PARQUET"}},
		}),
		mlPath2: encodeAvro(t, manifestListSchemaJSON, []iceberg.ManifestFile{}),
	})

	meta := &iceberg.TableMetadata{
		CurrentSnapshotID: 2,
		Snapshots: []iceberg.Snapshot{
			{SnapshotID: 1, ParentSnapshotID: 0, TimestampMs: ms(old), ManifestList: "s3://bucket/" + mlPath1},
			{SnapshotID: 2, ParentSnapshotID: 0, TimestampMs: ms(now), ManifestList: "s3://bucket/" + mlPath2},
		},
	}
	cat := &mockCatalog{meta: meta}

	e := NewEngine(cat, stor, retry.New(retry.Config{MaxAttempts: 3}))
	result, err := e.ExecuteExpiry(context.Background(), tableID, policy120())
	require.NoError(t, err)
	assert.Equal(t, 1, result.ExpiredSnapshots)
	assert.Equal(t, 1, cat.commitCalls)

	// Manifest list for snapshot 1 should be deleted.
	exists, _ := stor.Exists(context.Background(), mlPath1)
	assert.False(t, exists, "expired manifest list should be deleted")

	// Manifest list for snapshot 2 (current) should not be deleted.
	exists, _ = stor.Exists(context.Background(), mlPath2)
	assert.True(t, exists, "live manifest list should be preserved")

	// Exclusive data file of snapshot 1 should be deleted.
	exists, _ = stor.Exists(context.Background(), "table/data/snap-1-data.parquet")
	assert.False(t, exists, "expired data file should be deleted")
}

func TestExecuteExpiry_ConflictRetry(t *testing.T) {
	now := time.Now()
	old := now.Add(-200 * time.Hour)

	meta := &iceberg.TableMetadata{
		CurrentSnapshotID: 2,
		Snapshots: []iceberg.Snapshot{
			{SnapshotID: 1, ParentSnapshotID: 0, TimestampMs: ms(old), ManifestList: ""},
			{SnapshotID: 2, ParentSnapshotID: 0, TimestampMs: ms(now), ManifestList: ""},
		},
	}
	failOnce := &failOnceCatalog{meta: meta, failFirst: true}

	e := NewEngine(failOnce, testutil.NewMemStorage(nil), retry.New(retry.Config{MaxAttempts: 3}))
	result, err := e.ExecuteExpiry(context.Background(), tableID, policy120())
	require.NoError(t, err)
	assert.Equal(t, 1, result.ExpiredSnapshots)
	assert.Equal(t, 2, failOnce.commitCalls)
}

func TestExecuteExpiry_ConflictMaxRetries(t *testing.T) {
	now := time.Now()
	old := now.Add(-200 * time.Hour)

	cat := &mockCatalog{
		meta: &iceberg.TableMetadata{
			CurrentSnapshotID: 2,
			Snapshots: []iceberg.Snapshot{
				{SnapshotID: 1, ParentSnapshotID: 0, TimestampMs: ms(old), ManifestList: ""},
				{SnapshotID: 2, ParentSnapshotID: 0, TimestampMs: ms(now), ManifestList: ""},
			},
		},
		commitErr: catalog.ErrConflict{Table: tableID},
	}
	e := NewEngine(cat, testutil.NewMemStorage(nil), retry.New(retry.Config{MaxAttempts: 3}))
	_, err := e.ExecuteExpiry(context.Background(), tableID, policy120())
	require.Error(t, err)
	assert.Equal(t, 3, cat.commitCalls)
}

func TestExecuteExpiry_SharedManifestPreserved(t *testing.T) {
	now := time.Now()
	old := now.Add(-200 * time.Hour)

	// Both snapshots reference the same manifest.
	sharedManifestURI := "s3://bucket/table/metadata/shared-manifest.avro"
	mlPath1 := "table/metadata/snap-1-ml.avro"
	mlPath2 := "table/metadata/snap-2-ml.avro"
	sharedManifestPath := "table/metadata/shared-manifest.avro"

	stor := testutil.NewMemStorage(map[string][]byte{
		mlPath1: encodeAvro(t, manifestListSchemaJSON, []iceberg.ManifestFile{
			{Path: sharedManifestURI, AddedSnapshotID: 1},
		}),
		mlPath2: encodeAvro(t, manifestListSchemaJSON, []iceberg.ManifestFile{
			{Path: sharedManifestURI, AddedSnapshotID: 2},
		}),
		sharedManifestPath: encodeAvro(t, manifestEntrySchemaJSON, []iceberg.ManifestEntry{
			{Status: iceberg.EntryStatusAdded, DataFile: iceberg.DataFile{FilePath: "s3://bucket/data/f.parquet", FileFormat: "PARQUET"}},
		}),
	})

	cat := &mockCatalog{meta: &iceberg.TableMetadata{
		CurrentSnapshotID: 2,
		Snapshots: []iceberg.Snapshot{
			{SnapshotID: 1, ParentSnapshotID: 0, TimestampMs: ms(old), ManifestList: "s3://bucket/" + mlPath1},
			{SnapshotID: 2, ParentSnapshotID: 0, TimestampMs: ms(now), ManifestList: "s3://bucket/" + mlPath2},
		},
	}}

	e := NewEngine(cat, stor, retry.New(retry.Config{MaxAttempts: 3}))
	result, err := e.ExecuteExpiry(context.Background(), tableID, policy120())
	require.NoError(t, err)
	assert.Equal(t, 1, result.ExpiredSnapshots)

	// Shared manifest must NOT be deleted.
	exists, _ := stor.Exists(context.Background(), sharedManifestPath)
	assert.True(t, exists, "shared manifest must be preserved")

	// Expired manifest list (snap-1) should be deleted.
	exists, _ = stor.Exists(context.Background(), mlPath1)
	assert.False(t, exists, "expired manifest list should be deleted")
}

// failOnceCatalog fails the first CommitTransaction call with ErrConflict, then succeeds.
type failOnceCatalog struct {
	meta        *iceberg.TableMetadata
	failFirst   bool
	commitCalls int
}

func (f *failOnceCatalog) ListNamespaces(_ context.Context) ([]string, error) { return nil, nil }
func (f *failOnceCatalog) ListTables(_ context.Context, _ string) ([]catalog.TableIdentifier, error) {
	return nil, nil
}
func (f *failOnceCatalog) LoadTable(_ context.Context, _ catalog.TableIdentifier) (*iceberg.TableMetadata, error) {
	return f.meta, nil
}
func (f *failOnceCatalog) CommitTransaction(_ context.Context, id catalog.TableIdentifier, _ catalog.Transaction) error {
	f.commitCalls++
	if f.failFirst {
		f.failFirst = false
		return catalog.ErrConflict{Table: id}
	}
	return nil
}
