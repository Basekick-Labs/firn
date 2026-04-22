package azure

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/basekick-labs/firn/internal/config"
)

// Backend implements storage operations against Azure Blob Storage.
type Backend struct {
	container *container.Client
}

// RawConfig holds the flat fields needed to build an Azure client without
// importing the daemon config package. Used by the compact subprocess.
type RawConfig struct {
	Account          string
	Container        string
	AccountKey       string
	ConnectionString string
}

func New(ctx context.Context, cfg config.StorageConfig, containerName string) (*Backend, error) {
	c := containerName
	if c == "" {
		c = cfg.Container // fall back to the config-level default container
	}
	if c == "" {
		return nil, fmt.Errorf("azure: container name is required (set storage.container in config or pass it explicitly)")
	}
	return NewFromRaw(ctx, RawConfig{
		Account:          cfg.Account,
		Container:        c,
		AccountKey:       cfg.AccountKey,
		ConnectionString: cfg.ConnectionString,
	})
}

func NewFromRaw(ctx context.Context, cfg RawConfig) (*Backend, error) {
	var (
		svcClient *azblob.Client
		err       error
	)
	switch {
	case cfg.ConnectionString != "":
		svcClient, err = azblob.NewClientFromConnectionString(cfg.ConnectionString, nil)
	case cfg.AccountKey != "":
		cred, credErr := azblob.NewSharedKeyCredential(cfg.Account, cfg.AccountKey)
		if credErr != nil {
			return nil, fmt.Errorf("azure: shared key credential: %w", credErr)
		}
		svcURL := fmt.Sprintf("https://%s.blob.core.windows.net/", cfg.Account)
		svcClient, err = azblob.NewClientWithSharedKeyCredential(svcURL, cred, nil)
	default:
		// Managed identity / environment credentials (DefaultAzureCredential).
		var cred *azidentity.DefaultAzureCredential
		cred, err = azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("azure: default credential: %w", err)
		}
		svcURL := fmt.Sprintf("https://%s.blob.core.windows.net/", cfg.Account)
		svcClient, err = azblob.NewClient(svcURL, cred, nil)
	}
	if err != nil {
		return nil, fmt.Errorf("azure: create client: %w", err)
	}
	return &Backend{
		container: svcClient.ServiceClient().NewContainerClient(cfg.Container),
	}, nil
}

func (b *Backend) Read(ctx context.Context, path string) (io.ReadCloser, error) {
	resp, err := b.container.NewBlobClient(path).DownloadStream(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("azure read %s: %w", path, err)
	}
	return resp.Body, nil
}

func (b *Backend) ReadTo(ctx context.Context, path string, w io.Writer) error {
	rc, err := b.Read(ctx, path)
	if err != nil {
		return err
	}
	defer rc.Close()
	_, err = io.Copy(w, rc)
	return err
}

func (b *Backend) Write(ctx context.Context, path string, r io.Reader, _ int64) error {
	_, err := b.container.NewBlockBlobClient(path).UploadStream(ctx, r, nil)
	if err != nil {
		return fmt.Errorf("azure write %s: %w", path, err)
	}
	return nil
}

func (b *Backend) Delete(ctx context.Context, path string) error {
	_, err := b.container.NewBlobClient(path).Delete(ctx, nil)
	if err != nil {
		return fmt.Errorf("azure delete %s: %w", path, err)
	}
	return nil
}

func (b *Backend) Exists(ctx context.Context, path string) (bool, error) {
	_, err := b.container.NewBlobClient(path).GetProperties(ctx, nil)
	if err != nil {
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) && respErr.StatusCode == http.StatusNotFound {
			return false, nil
		}
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("azure exists %s: %w", path, err)
	}
	return true, nil
}

func (b *Backend) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	pager := b.container.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix: &prefix,
	})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("azure list %s: %w", prefix, err)
		}
		for _, item := range page.Segment.BlobItems {
			if item.Name != nil {
				keys = append(keys, *item.Name)
			}
		}
	}
	return keys, nil
}

func (b *Backend) StatFile(ctx context.Context, path string) (int64, error) {
	props, err := b.container.NewBlobClient(path).GetProperties(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("azure stat %s: %w", path, err)
	}
	if props.ContentLength == nil {
		return 0, fmt.Errorf("azure stat %s: no ContentLength", path)
	}
	return *props.ContentLength, nil
}

func (b *Backend) ModTime(ctx context.Context, path string) (time.Time, error) {
	props, err := b.container.NewBlobClient(path).GetProperties(ctx, nil)
	if err != nil {
		return time.Time{}, fmt.Errorf("azure modtime %s: %w", path, err)
	}
	if props.LastModified == nil {
		return time.Time{}, fmt.Errorf("azure modtime %s: no LastModified", path)
	}
	return *props.LastModified, nil
}
