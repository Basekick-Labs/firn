package catalog

import (
	"context"

	"github.com/basekick-labs/firn/internal/iceberg"
)

// Client is the interface every catalog implementation must satisfy.
type Client interface {
	// ListNamespaces returns all namespaces in the catalog.
	ListNamespaces(ctx context.Context) ([]string, error)

	// ListTables returns all tables in the given namespace.
	ListTables(ctx context.Context, namespace string) ([]TableIdentifier, error)

	// LoadTable returns the current metadata for a table.
	LoadTable(ctx context.Context, id TableIdentifier) (*iceberg.TableMetadata, error)

	// CommitTransaction atomically commits a set of updates to a table.
	// Returns ErrConflict if another writer committed concurrently.
	CommitTransaction(ctx context.Context, id TableIdentifier, tx Transaction) error
}

type TableIdentifier struct {
	Namespace string
	Name      string
}

func (t TableIdentifier) String() string {
	return t.Namespace + "." + t.Name
}

// Transaction represents an atomic set of metadata changes to commit.
type Transaction struct {
	// Requirements that must hold for the commit to succeed.
	Requirements []Requirement
	// Updates to apply atomically.
	Updates []Update
}

type Requirement struct {
	Type              string // "assert-current-snapshot-id"
	CurrentSnapshotID int64
}

type Update struct {
	Type     string // "add-snapshot", "set-snapshot-ref", "remove-snapshots"
	Snapshot *iceberg.Snapshot
	RefName  string
	SnapshotIDs []int64
}

// ErrConflict is returned when a catalog commit fails due to concurrent modification.
type ErrConflict struct {
	Table TableIdentifier
}

func (e ErrConflict) Error() string {
	return "catalog conflict on table " + e.Table.String() + ": retry required"
}
