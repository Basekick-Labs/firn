// Package testutil provides shared test helpers.
package testutil

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"
)

// MemStorage is an in-memory storage.Backend for use in tests.
type MemStorage struct {
	mu         sync.RWMutex
	files      map[string][]byte
	writeTimes map[string]time.Time
}

func NewMemStorage(files map[string][]byte) *MemStorage {
	if files == nil {
		files = map[string][]byte{}
	}
	// Pre-populate write times for seed files as 48h ago so they are old enough
	// to pass any default grace period check in orphan cleanup tests.
	wt := make(map[string]time.Time, len(files))
	old := time.Now().Add(-48 * time.Hour)
	for k := range files {
		wt[k] = old
	}
	return &MemStorage{files: files, writeTimes: wt}
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
	m.writeTimes[path] = time.Now()
	return nil
}

func (m *MemStorage) ModTime(_ context.Context, path string) (time.Time, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.writeTimes[path]
	if !ok {
		return time.Time{}, fmt.Errorf("not found: %s", path)
	}
	return t, nil
}

// WriteOld writes data to path with a write time of now-age. Used in tests to
// simulate files that are older than a grace period.
func (m *MemStorage) WriteOld(path string, data []byte, age time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[path] = data
	m.writeTimes[path] = time.Now().Add(-age)
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
