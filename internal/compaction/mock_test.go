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

// panicCatalog panics on LoadTable to verify that validation guards short-circuit before catalog I/O.
type panicCatalog struct{}

func (p *panicCatalog) ListNamespaces(_ context.Context) ([]string, error) { return nil, nil }
func (p *panicCatalog) ListTables(_ context.Context, _ string) ([]catalog.TableIdentifier, error) {
	return nil, nil
}
func (p *panicCatalog) LoadTable(_ context.Context, _ catalog.TableIdentifier) (*iceberg.TableMetadata, error) {
	panic("LoadTable must not be called before validation")
}
func (p *panicCatalog) CommitTransaction(_ context.Context, _ catalog.TableIdentifier, _ catalog.Transaction) error {
	panic("CommitTransaction must not be called before validation")
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
