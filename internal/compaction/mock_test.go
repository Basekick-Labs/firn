package compaction

import (
	"context"

	"github.com/basekick-labs/firn/internal/catalog"
	"github.com/basekick-labs/firn/internal/iceberg"
)

// mockCatalog is a test double for catalog.Client.
type mockCatalog struct {
	meta        *iceberg.TableMetadata
	commitErr   error
	commitCalls int
}

func (m *mockCatalog) ListNamespaces(_ context.Context) ([]string, error) { return nil, nil }
func (m *mockCatalog) ListTables(_ context.Context, _ string) ([]catalog.TableIdentifier, error) {
	return nil, nil
}
func (m *mockCatalog) LoadTable(_ context.Context, _ catalog.TableIdentifier) (*iceberg.TableMetadata, error) {
	return m.meta, nil
}
func (m *mockCatalog) CommitTransaction(_ context.Context, _ catalog.TableIdentifier, _ catalog.Transaction) error {
	m.commitCalls++
	return m.commitErr
}
