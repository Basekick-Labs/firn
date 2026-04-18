// Package testutil provides shared test helpers.
package testutil

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
)

// MemStorage is an in-memory storage.Backend for use in tests.
type MemStorage struct {
	mu    sync.RWMutex
	files map[string][]byte
}

func NewMemStorage(files map[string][]byte) *MemStorage {
	if files == nil {
		files = map[string][]byte{}
	}
	return &MemStorage{files: files}
}

func (m *MemStorage) Read(_ context.Context, path string) (io.ReadCloser, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.files[path]
	if !ok {
		return nil, fmt.Errorf("not found: %s", path)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *MemStorage) ReadTo(ctx context.Context, path string, w io.Writer) error {
	rc, err := m.Read(ctx, path)
	if err != nil {
		return err
	}
	defer rc.Close()
	_, err = io.Copy(w, rc)
	return err
}

func (m *MemStorage) Write(_ context.Context, path string, r io.Reader, _ int64) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[path] = data
	return nil
}

func (m *MemStorage) Delete(_ context.Context, path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.files, path)
	return nil
}

func (m *MemStorage) Exists(_ context.Context, path string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.files[path]
	return ok, nil
}

func (m *MemStorage) List(_ context.Context, prefix string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var keys []string
	for k := range m.files {
		if len(prefix) == 0 || len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func (m *MemStorage) StatFile(_ context.Context, path string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.files[path]
	if !ok {
		return 0, fmt.Errorf("not found: %s", path)
	}
	return int64(len(data)), nil
}
