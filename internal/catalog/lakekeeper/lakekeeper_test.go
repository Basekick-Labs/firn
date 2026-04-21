package lakekeeper_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/basekick-labs/firn/internal/catalog/lakekeeper"
	"github.com/basekick-labs/firn/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNew verifies the constructor wires the correct base URL and token endpoint.
// Full behavioral coverage lives in internal/catalog/rest/client_test.go.
func TestNew_ReturnsClient(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/namespaces", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"namespaces":[]}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	c := lakekeeper.New(config.CatalogConfig{URL: srv.URL})
	require.NotNil(t, c)

	ns, err := c.ListNamespaces(t.Context())
	require.NoError(t, err)
	assert.Empty(t, ns)
}

func TestNew_TokenURIOverride(t *testing.T) {
	tokenCalled := false
	mux := http.NewServeMux()
	mux.HandleFunc("/custom/token", func(w http.ResponseWriter, r *http.Request) {
		tokenCalled = true
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"access_token":"tok","expires_in":3600}`))
	})
	mux.HandleFunc("/v1/namespaces", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"namespaces":[]}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	c := lakekeeper.New(config.CatalogConfig{
		URL: srv.URL,
		Credential: config.CatalogCredential{
			ClientID:     "id",
			ClientSecret: "secret",
			TokenURI:     srv.URL + "/custom/token",
		},
	})
	_, err := c.ListNamespaces(t.Context())
	require.NoError(t, err)
	assert.True(t, tokenCalled, "custom token URI should be used")
}
