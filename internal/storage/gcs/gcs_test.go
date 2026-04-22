package gcs_test

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/basekick-labs/firn/internal/storage/gcs"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testBucket = "test-bucket"

func newBackend(t *testing.T) *gcs.Backend {
	t.Helper()
	srv := fakestorage.NewServer(nil)
	t.Cleanup(srv.Stop)
	srv.CreateBucket(testBucket)
	return gcs.NewWithClient(srv.Client(), testBucket)
}

func TestGCS_WriteRead(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	content := []byte("hello from GCS")
	err := b.Write(ctx, "data/file.parquet", bytes.NewReader(content), int64(len(content)))
	require.NoError(t, err)

	rc, err := b.Read(ctx, "data/file.parquet")
	require.NoError(t, err)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, content, got)
}

func TestGCS_ReadTo(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	content := []byte("readto content")
	require.NoError(t, b.Write(ctx, "obj", bytes.NewReader(content), int64(len(content))))

	var buf strings.Builder
	require.NoError(t, b.ReadTo(ctx, "obj", &buf))
	assert.Equal(t, string(content), buf.String())
}

func TestGCS_Exists(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	exists, err := b.Exists(ctx, "missing")
	require.NoError(t, err)
	assert.False(t, exists)

	require.NoError(t, b.Write(ctx, "present", bytes.NewReader([]byte("x")), 1))

	exists, err = b.Exists(ctx, "present")
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestGCS_Delete(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	require.NoError(t, b.Write(ctx, "todelete", bytes.NewReader([]byte("bye")), 3))
	require.NoError(t, b.Delete(ctx, "todelete"))

	exists, err := b.Exists(ctx, "todelete")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestGCS_List(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	files := []string{"ns/t1/a.parquet", "ns/t1/b.parquet", "ns/t2/c.parquet"}
	for _, f := range files {
		require.NoError(t, b.Write(ctx, f, bytes.NewReader([]byte("data")), 4))
	}

	got, err := b.List(ctx, "ns/t1/")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"ns/t1/a.parquet", "ns/t1/b.parquet"}, got)

	all, err := b.List(ctx, "ns/")
	require.NoError(t, err)
	assert.ElementsMatch(t, files, all)
}

func TestGCS_StatFile(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	content := []byte("stat me")
	require.NoError(t, b.Write(ctx, "stat.parquet", bytes.NewReader(content), int64(len(content))))

	size, err := b.StatFile(ctx, "stat.parquet")
	require.NoError(t, err)
	assert.Equal(t, int64(len(content)), size)
}

func TestGCS_ModTime(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	require.NoError(t, b.Write(ctx, "mod.parquet", bytes.NewReader([]byte("x")), 1))

	mt, err := b.ModTime(ctx, "mod.parquet")
	require.NoError(t, err)
	assert.False(t, mt.IsZero())
}
