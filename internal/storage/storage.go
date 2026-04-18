package storage

import (
	"context"
	"io"
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
