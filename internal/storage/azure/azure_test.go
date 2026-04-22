package azure_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/basekick-labs/firn/internal/storage/azure"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	azuriteImage    = "mcr.microsoft.com/azure-storage/azurite:latest"
	testContainer   = "test-container"
	azuriteAccount  = "devstoreaccount1"
	azuriteKey      = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)

func startAzurite(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	req := testcontainers.ContainerRequest{
		Image:        azuriteImage,
		ExposedPorts: []string{"10000/tcp"},
		Cmd:          []string{"azurite-blob", "--blobHost", "0.0.0.0", "--skipApiVersionCheck"},
		WaitingFor:   wait.ForListeningPort("10000/tcp").WithStartupTimeout(60 * time.Second),
	}
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Skipf("docker not available, skipping Azure integration test: %v", err)
		return ""
	}
	testcontainers.CleanupContainer(t, ctr)

	host, err := ctr.Host(ctx)
	require.NoError(t, err)
	port, err := ctr.MappedPort(ctx, "10000")
	require.NoError(t, err)

	return fmt.Sprintf(
		"DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=http://%s:%s/%s;",
		azuriteAccount, azuriteKey, host, port.Port(), azuriteAccount,
	)
}

func newBackend(t *testing.T) *azure.Backend {
	t.Helper()
	connStr := startAzurite(t)

	// Create the container via the SDK before running tests.
	svc, err := azblob.NewClientFromConnectionString(connStr, nil)
	require.NoError(t, err)
	_, err = svc.CreateContainer(context.Background(), testContainer, nil)
	require.NoError(t, err)

	b, err := azure.NewFromRaw(context.Background(), azure.RawConfig{
		Account:          azuriteAccount,
		Container:        testContainer,
		ConnectionString: connStr,
	})
	require.NoError(t, err)
	return b
}

func TestAzure_WriteRead(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	content := []byte("hello from Azure")
	err := b.Write(ctx, "data/file.parquet", bytes.NewReader(content), int64(len(content)))
	require.NoError(t, err)

	rc, err := b.Read(ctx, "data/file.parquet")
	require.NoError(t, err)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, content, got)
}

func TestAzure_ReadTo(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	content := []byte("readto content")
	require.NoError(t, b.Write(ctx, "obj", bytes.NewReader(content), int64(len(content))))

	var buf strings.Builder
	require.NoError(t, b.ReadTo(ctx, "obj", &buf))
	assert.Equal(t, string(content), buf.String())
}

func TestAzure_Exists(t *testing.T) {
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

func TestAzure_Delete(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	require.NoError(t, b.Write(ctx, "todelete", bytes.NewReader([]byte("bye")), 3))
	require.NoError(t, b.Delete(ctx, "todelete"))

	exists, err := b.Exists(ctx, "todelete")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestAzure_List(t *testing.T) {
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

func TestAzure_StatFile(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	content := []byte("stat me")
	require.NoError(t, b.Write(ctx, "stat.parquet", bytes.NewReader(content), int64(len(content))))

	size, err := b.StatFile(ctx, "stat.parquet")
	require.NoError(t, err)
	assert.Equal(t, int64(len(content)), size)
}

func TestAzure_ModTime(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	require.NoError(t, b.Write(ctx, "mod.parquet", bytes.NewReader([]byte("x")), 1))

	mt, err := b.ModTime(ctx, "mod.parquet")
	require.NoError(t, err)
	assert.False(t, mt.IsZero())
}
