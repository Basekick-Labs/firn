package glue

import (
	"context"
	"fmt"

	"github.com/basekick-labs/firn/internal/catalog"
	"github.com/basekick-labs/firn/internal/config"
	"github.com/basekick-labs/firn/internal/iceberg"
)

// Client implements catalog.Client against AWS Glue Data Catalog
// via its Iceberg REST endpoint.
type Client struct {
	region string
}

func New(ctx context.Context, cfg config.CatalogConfig) (*Client, error) {
	return &Client{region: cfg.Region}, nil
}

func (c *Client) ListNamespaces(ctx context.Context) ([]string, error) {
	// TODO: call Glue ListDatabases and return as namespaces
	return nil, fmt.Errorf("glue: ListNamespaces not yet implemented")
}

func (c *Client) ListTables(ctx context.Context, namespace string) ([]catalog.TableIdentifier, error) {
	// TODO: call Glue ListTables for database=namespace
	return nil, fmt.Errorf("glue: ListTables not yet implemented")
}

func (c *Client) LoadTable(ctx context.Context, id catalog.TableIdentifier) (*iceberg.TableMetadata, error) {
	// TODO: call Glue GetTable and parse Iceberg metadata location from
	// table parameters, then read metadata.json from S3
	return nil, fmt.Errorf("glue: LoadTable not yet implemented")
}

func (c *Client) CommitTransaction(ctx context.Context, id catalog.TableIdentifier, tx catalog.Transaction) error {
	// TODO: update Glue table parameters with new metadata location
	// after writing new metadata.json to S3
	return fmt.Errorf("glue: CommitTransaction not yet implemented")
}
