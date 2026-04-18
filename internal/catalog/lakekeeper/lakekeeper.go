package lakekeeper

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/basekick-labs/firn/internal/catalog"
	"github.com/basekick-labs/firn/internal/config"
	"github.com/basekick-labs/firn/internal/iceberg"
)

// Client implements catalog.Client against a Lakekeeper REST catalog.
type Client struct {
	baseURL string
	http    *http.Client
}

func New(cfg config.CatalogConfig) *Client {
	return &Client{
		baseURL: cfg.URL,
		http:    &http.Client{},
	}
}

func (c *Client) ListNamespaces(ctx context.Context) ([]string, error) {
	// TODO: GET /v1/namespaces
	return nil, fmt.Errorf("lakekeeper: ListNamespaces not yet implemented")
}

func (c *Client) ListTables(ctx context.Context, namespace string) ([]catalog.TableIdentifier, error) {
	// TODO: GET /v1/namespaces/{namespace}/tables
	return nil, fmt.Errorf("lakekeeper: ListTables not yet implemented")
}

func (c *Client) LoadTable(ctx context.Context, id catalog.TableIdentifier) (*iceberg.TableMetadata, error) {
	// TODO: GET /v1/namespaces/{namespace}/tables/{table}
	_ = ctx
	_ = id
	return nil, fmt.Errorf("lakekeeper: LoadTable not yet implemented")
}

func (c *Client) CommitTransaction(ctx context.Context, id catalog.TableIdentifier, tx catalog.Transaction) error {
	// TODO: POST /v1/namespaces/{namespace}/tables/{table}/transactions/commit
	body, err := json.Marshal(tx)
	if err != nil {
		return err
	}
	_ = body
	return fmt.Errorf("lakekeeper: CommitTransaction not yet implemented")
}
