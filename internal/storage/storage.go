package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/basekick-labs/firn/internal/storage/s3"
)

// Backend is the interface all storage implementations must satisfy.
// All paths use forward slashes regardless of operating system.
type Backend interface {
	Read(ctx context.Context, path string) (io.ReadCloser, error)
	ReadTo(ctx context.Context, path string, w io.Writer) error
	Write(ctx context.Context, path string, r io.Reader, size int64) error
	Delete(ctx context.Context, path string) error
	Exists(ctx context.Context, path string) (bool, error)
	List(ctx context.Context, prefix string) ([]string, error)
	StatFile(ctx context.Context, path string) (int64, error)
}

// S3Config is the JSON-serializable subset of s3 backend configuration
// used when passing credentials to the compact subprocess.
type S3Config struct {
	Bucket          string `json:"bucket"`
	Endpoint        string `json:"endpoint,omitempty"`
	Region          string `json:"region,omitempty"`
	AccessKeyID     string `json:"access_key_id,omitempty"`
	SecretAccessKey string `json:"secret_access_key,omitempty"`
	PathStyle       bool   `json:"path_style,omitempty"`
}

// FromConfig constructs a Backend from a storage type name and a JSON-encoded
// config blob. Used by the compact subprocess to avoid importing daemon config.
func FromConfig(ctx context.Context, storageType, configJSON string) (Backend, error) {
	switch storageType {
	case "s3":
		var sc S3Config
		if err := json.Unmarshal([]byte(configJSON), &sc); err != nil {
			return nil, fmt.Errorf("parse s3 config: %w", err)
		}
		return s3.NewFromRaw(ctx, s3.RawConfig{
			Bucket:          sc.Bucket,
			Endpoint:        sc.Endpoint,
			Region:          sc.Region,
			AccessKeyID:     sc.AccessKeyID,
			SecretAccessKey: sc.SecretAccessKey,
			PathStyle:       sc.PathStyle,
		})
	default:
		return nil, fmt.Errorf("unsupported storage type: %q", storageType)
	}
}
