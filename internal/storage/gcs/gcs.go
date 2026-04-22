package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	gcslib "cloud.google.com/go/storage"
	"github.com/basekick-labs/firn/internal/config"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// Backend implements storage operations against Google Cloud Storage.
type Backend struct {
	client *gcslib.Client
	bucket string
}

// RawConfig holds the flat fields needed to build a GCS client without
// importing the daemon config package. Used by the compact subprocess.
type RawConfig struct {
	Bucket          string
	Project         string
	CredentialsJSON string // raw service-account JSON; uses ADC if empty
}

func New(ctx context.Context, cfg config.StorageConfig, bucket string) (*Backend, error) {
	return NewFromRaw(ctx, RawConfig{
		Bucket:          bucket,
		Project:         cfg.Project,
		CredentialsJSON: cfg.CredentialsJSON,
	})
}

func NewFromRaw(ctx context.Context, cfg RawConfig) (*Backend, error) {
	var opts []option.ClientOption
	if cfg.CredentialsJSON != "" {
		opts = append(opts, option.WithCredentialsJSON([]byte(cfg.CredentialsJSON)))
	}
	// No explicit credentials → GCS SDK falls back to Application Default Credentials.
	client, err := gcslib.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("gcs: create client: %w", err)
	}
	return &Backend{client: client, bucket: cfg.Bucket}, nil
}

// NewWithClient creates a Backend from a pre-built GCS client. Used in tests.
func NewWithClient(client *gcslib.Client, bucket string) *Backend {
	return &Backend{client: client, bucket: bucket}
}

func (b *Backend) Read(ctx context.Context, path string) (io.ReadCloser, error) {
	rc, err := b.client.Bucket(b.bucket).Object(path).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("gcs read %s: %w", path, err)
	}
	return rc, nil
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
	wc := b.client.Bucket(b.bucket).Object(path).NewWriter(ctx)
	if _, err := io.Copy(wc, r); err != nil {
		_ = wc.Close()
		return fmt.Errorf("gcs write %s: %w", path, err)
	}
	// Close commits the object; errors here mean the write did not land.
	if err := wc.Close(); err != nil {
		return fmt.Errorf("gcs write %s: commit: %w", path, err)
	}
	return nil
}

func (b *Backend) Delete(ctx context.Context, path string) error {
	if err := b.client.Bucket(b.bucket).Object(path).Delete(ctx); err != nil {
		return fmt.Errorf("gcs delete %s: %w", path, err)
	}
	return nil
}

func (b *Backend) Exists(ctx context.Context, path string) (bool, error) {
	_, err := b.client.Bucket(b.bucket).Object(path).Attrs(ctx)
	if errors.Is(err, gcslib.ErrObjectNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("gcs exists %s: %w", path, err)
	}
	return true, nil
}

func (b *Backend) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	it := b.client.Bucket(b.bucket).Objects(ctx, &gcslib.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("gcs list %s: %w", prefix, err)
		}
		keys = append(keys, attrs.Name)
	}
	return keys, nil
}

func (b *Backend) StatFile(ctx context.Context, path string) (int64, error) {
	attrs, err := b.client.Bucket(b.bucket).Object(path).Attrs(ctx)
	if err != nil {
		return 0, fmt.Errorf("gcs stat %s: %w", path, err)
	}
	return attrs.Size, nil
}

func (b *Backend) ModTime(ctx context.Context, path string) (time.Time, error) {
	attrs, err := b.client.Bucket(b.bucket).Object(path).Attrs(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("gcs modtime %s: %w", path, err)
	}
	return attrs.Updated, nil
}
