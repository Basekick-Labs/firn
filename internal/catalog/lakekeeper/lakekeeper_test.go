package lakekeeper

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/basekick-labs/firn/internal/catalog"
	"github.com/basekick-labs/firn/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestClient(t *testing.T, mux *http.ServeMux) *Client {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return New(config.CatalogConfig{URL: srv.URL})
}

func newTestClientWithAuth(t *testing.T, mux *http.ServeMux) *Client {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return New(config.CatalogConfig{
		URL: srv.URL,
		Credential: config.CatalogCredential{
			ClientID:     "test-client",
			ClientSecret: "test-secret",
		},
	})
}

// --- ListNamespaces ---

func TestListNamespaces_Success(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/namespaces", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		json.NewEncoder(w).Encode(map[string]any{
			"namespaces": [][]string{{"analytics"}, {"finance"}},
		})
	})

	c := newTestClient(t, mux)
	ns, err := c.ListNamespaces(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"analytics", "finance"}, ns)
}

func TestListNamespaces_Pagination(t *testing.T) {
	calls := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/namespaces", func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Query().Get("pageToken") == "" {
			json.NewEncoder(w).Encode(map[string]any{
				"namespaces":      [][]string{{"ns1"}},
				"next-page-token": "tok2",
			})
		} else {
			json.NewEncoder(w).Encode(map[string]any{
				"namespaces": [][]string{{"ns2"}},
			})
		}
	})

	c := newTestClient(t, mux)
	ns, err := c.ListNamespaces(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"ns1", "ns2"}, ns)
	assert.Equal(t, 2, calls)
}

func TestListNamespaces_MultipartNamespace(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/namespaces", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"namespaces": [][]string{{"company", "analytics"}},
		})
	})

	c := newTestClient(t, mux)
	ns, err := c.ListNamespaces(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"company.analytics"}, ns)
}

func TestListNamespaces_ServerError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/namespaces", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	})

	c := newTestClient(t, mux)
	_, err := c.ListNamespaces(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}

// --- ListTables ---

func TestListTables_Success(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/namespaces/analytics/tables", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"identifiers": []map[string]any{
				{"namespace": []string{"analytics"}, "name": "orders"},
				{"namespace": []string{"analytics"}, "name": "customers"},
			},
		})
	})

	c := newTestClient(t, mux)
	tables, err := c.ListTables(context.Background(), "analytics")
	require.NoError(t, err)
	assert.Equal(t, []catalog.TableIdentifier{
		{Namespace: "analytics", Name: "orders"},
		{Namespace: "analytics", Name: "customers"},
	}, tables)
}

func TestListTables_NotFound(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/namespaces/missing/tables", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "namespace not found", http.StatusNotFound)
	})

	c := newTestClient(t, mux)
	tables, err := c.ListTables(context.Background(), "missing")
	require.NoError(t, err)
	assert.Nil(t, tables)
}

func TestListTables_Pagination(t *testing.T) {
	calls := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/namespaces/ns/tables", func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Query().Get("pageToken") == "" {
			json.NewEncoder(w).Encode(map[string]any{
				"identifiers":     []map[string]any{{"namespace": []string{"ns"}, "name": "t1"}},
				"next-page-token": "page2",
			})
		} else {
			json.NewEncoder(w).Encode(map[string]any{
				"identifiers": []map[string]any{{"namespace": []string{"ns"}, "name": "t2"}},
			})
		}
	})

	c := newTestClient(t, mux)
	tables, err := c.ListTables(context.Background(), "ns")
	require.NoError(t, err)
	assert.Len(t, tables, 2)
	assert.Equal(t, 2, calls)
}

// --- LoadTable ---

func TestLoadTable_Success(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/namespaces/analytics/tables/orders", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"metadata-location": "s3://bucket/path/metadata.json",
			"metadata": map[string]any{
				"format-version":       2,
				"table-uuid":           "abc-123",
				"location":             "s3://bucket/tables/orders",
				"last-sequence-number": 5,
				"last-updated-ms":      1700000000000,
				"current-snapshot-id":  int64(42),
				"snapshots": []map[string]any{
					{
						"snapshot-id":    int64(42),
						"sequence-number": 1,
						"timestamp-ms":   1700000000000,
						"manifest-list":  "s3://bucket/manifests/snap-42.avro",
					},
				},
			},
		})
	})

	c := newTestClient(t, mux)
	meta, err := c.LoadTable(context.Background(), catalog.TableIdentifier{
		Namespace: "analytics",
		Name:      "orders",
	})
	require.NoError(t, err)
	assert.Equal(t, 2, meta.FormatVersion)
	assert.Equal(t, "abc-123", meta.TableUUID)
	assert.Equal(t, int64(42), meta.CurrentSnapshotID)
	assert.Len(t, meta.Snapshots, 1)
	assert.Equal(t, "s3://bucket/manifests/snap-42.avro", meta.Snapshots[0].ManifestList)
}

func TestLoadTable_NotFound(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/namespaces/ns/tables/missing", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	})

	c := newTestClient(t, mux)
	_, err := c.LoadTable(context.Background(), catalog.TableIdentifier{Namespace: "ns", Name: "missing"})
	require.Error(t, err)
	var notFound ErrTableNotFound
	assert.ErrorAs(t, err, &notFound)
	assert.Equal(t, "ns.missing", notFound.Table.String())
}

// --- CommitTransaction ---

func TestCommitTransaction_Success(t *testing.T) {
	var received map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/namespaces/ns/tables/tbl/transactions/commit", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.NotEmpty(t, r.Header.Get("Idempotency-Key"))
		json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(http.StatusNoContent)
	})

	c := newTestClient(t, mux)
	err := c.CommitTransaction(context.Background(),
		catalog.TableIdentifier{Namespace: "ns", Name: "tbl"},
		catalog.Transaction{
			Requirements: []catalog.Requirement{{Type: "assert-current-snapshot-id", CurrentSnapshotID: 42}},
			Updates:      []catalog.Update{{Type: "add-snapshot"}},
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, received["requirements"])
	assert.NotNil(t, received["updates"])
}

func TestCommitTransaction_Conflict(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/namespaces/ns/tables/tbl/transactions/commit", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "requirement failed", http.StatusConflict)
	})

	c := newTestClient(t, mux)
	err := c.CommitTransaction(context.Background(),
		catalog.TableIdentifier{Namespace: "ns", Name: "tbl"},
		catalog.Transaction{},
	)
	require.Error(t, err)
	var conflict catalog.ErrConflict
	assert.ErrorAs(t, err, &conflict)
	assert.Equal(t, "ns.tbl", conflict.Table.String())
}

// --- Auth ---

func TestAuth_TokenFetchedAndCached(t *testing.T) {
	tokenCalls := 0
	mux := http.NewServeMux()

	mux.HandleFunc("/oauth/tokens", func(w http.ResponseWriter, r *http.Request) {
		tokenCalls++
		assert.Equal(t, "client_credentials", r.FormValue("grant_type"))
		assert.Equal(t, "test-client", r.FormValue("client_id"))
		json.NewEncoder(w).Encode(map[string]any{
			"access_token": "tok-abc",
			"expires_in":   3600,
		})
	})

	mux.HandleFunc("/v1/namespaces", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer tok-abc", r.Header.Get("Authorization"))
		json.NewEncoder(w).Encode(map[string]any{"namespaces": [][]string{}})
	})

	c := newTestClientWithAuth(t, mux)
	ctx := context.Background()

	// Two calls — token should only be fetched once.
	_, err := c.ListNamespaces(ctx)
	require.NoError(t, err)
	_, err = c.ListNamespaces(ctx)
	require.NoError(t, err)

	assert.Equal(t, 1, tokenCalls, "token should be cached and only fetched once")
}

func TestAuth_TokenRefreshedWhenExpired(t *testing.T) {
	tokenCalls := 0
	mux := http.NewServeMux()

	mux.HandleFunc("/oauth/tokens", func(w http.ResponseWriter, r *http.Request) {
		tokenCalls++
		json.NewEncoder(w).Encode(map[string]any{
			"access_token": "tok-" + string(rune('a'+tokenCalls)),
			"expires_in":   30, // 30s — minus 30s buffer = already expired
		})
	})

	mux.HandleFunc("/v1/namespaces", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"namespaces": [][]string{}})
	})

	c := newTestClientWithAuth(t, mux)
	ctx := context.Background()

	// Force first token to appear expired immediately.
	c.tokens.expiresAt = time.Now().Add(-1 * time.Second)

	_, err := c.ListNamespaces(ctx)
	require.NoError(t, err)
	_, err = c.ListNamespaces(ctx)
	require.NoError(t, err)

	assert.Equal(t, 2, tokenCalls, "token should be refreshed on each call when expired")
}

func TestAuth_Failure(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/oauth/tokens", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	})

	c := newTestClientWithAuth(t, mux)
	_, err := c.ListNamespaces(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "token request HTTP 401")
}

// --- encodeNamespace ---

func TestEncodeNamespace(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"analytics", "analytics"},
		// url.PathEscape encodes \x1F (unit separator) as %1F.
		{"company.analytics", "company%1Fanalytics"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, encodeNamespace(tt.input))
	}
}
