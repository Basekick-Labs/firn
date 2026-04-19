package iceberg

import (
	"context"
	"testing"

	"github.com/basekick-labs/firn/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteManifest_RoundTrip(t *testing.T) {
	snap1 := int64(42)
	entries := []ManifestEntry{
		{Status: EntryStatusAdded, SnapshotID: &snap1, DataFile: DataFile{FilePath: "data/a.parquet", FileFormat: "PARQUET", RecordCount: 100, FileSizeBytes: 1024}},
		{Status: EntryStatusExisting, SnapshotID: &snap1, DataFile: DataFile{FilePath: "data/b.parquet", FileFormat: "PARQUET", RecordCount: 200, FileSizeBytes: 2048}},
	}

	stor := testutil.NewMemStorage(nil)
	require.NoError(t, WriteManifest(context.Background(), stor, "test.avro", entries))

	got, err := ReadManifest(context.Background(), stor, "test.avro")
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "data/a.parquet", got[0].DataFile.FilePath)
	assert.Equal(t, int64(100), got[0].DataFile.RecordCount)
	assert.Equal(t, "data/b.parquet", got[1].DataFile.FilePath)
	assert.Equal(t, int64(2048), got[1].DataFile.FileSizeBytes)
}

func TestWriteManifest_Empty(t *testing.T) {
	stor := testutil.NewMemStorage(nil)
	require.NoError(t, WriteManifest(context.Background(), stor, "empty.avro", nil))

	got, err := ReadManifest(context.Background(), stor, "empty.avro")
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestWriteManifest_FilterDeletedOnRead(t *testing.T) {
	snap1 := int64(1)
	entries := []ManifestEntry{
		{Status: EntryStatusAdded, SnapshotID: &snap1, DataFile: DataFile{FilePath: "a.parquet", FileFormat: "PARQUET"}},
		{Status: EntryStatusDeleted, SnapshotID: &snap1, DataFile: DataFile{FilePath: "b.parquet", FileFormat: "PARQUET"}},
	}
	stor := testutil.NewMemStorage(nil)
	require.NoError(t, WriteManifest(context.Background(), stor, "m.avro", entries))

	// ReadManifest filters deleted entries.
	got, err := ReadManifest(context.Background(), stor, "m.avro")
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "a.parquet", got[0].DataFile.FilePath)
}

func TestWriteManifestList_RoundTrip(t *testing.T) {
	files := []ManifestFile{
		{Path: "meta/m1.avro", Content: ManifestContentData, AddedSnapshotID: 10, AddedFilesCount: 3, AddedRowsCount: 300},
		{Path: "meta/m2.avro", Content: ManifestContentData, AddedSnapshotID: 11, AddedFilesCount: 1, AddedRowsCount: 100},
	}

	stor := testutil.NewMemStorage(nil)
	require.NoError(t, WriteManifestList(context.Background(), stor, "ml.avro", files))

	got, err := ReadManifestList(context.Background(), stor, "ml.avro")
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "meta/m1.avro", got[0].Path)
	assert.Equal(t, int64(10), got[0].AddedSnapshotID)
	assert.Equal(t, "meta/m2.avro", got[1].Path)
}

func TestWriteManifestList_FiltersDeleteManifests(t *testing.T) {
	files := []ManifestFile{
		{Path: "data.avro", Content: ManifestContentData, AddedSnapshotID: 1},
		{Path: "deletes.avro", Content: ManifestContentDeletes, AddedSnapshotID: 1},
	}
	stor := testutil.NewMemStorage(nil)
	require.NoError(t, WriteManifestList(context.Background(), stor, "ml.avro", files))

	// ReadManifestList excludes delete manifests.
	got, err := ReadManifestList(context.Background(), stor, "ml.avro")
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "data.avro", got[0].Path)
}
