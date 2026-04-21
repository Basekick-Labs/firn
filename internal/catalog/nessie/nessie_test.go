package nessie_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/basekick-labs/firn/internal/catalog/nessie"
	"github.com/basekick-labs/firn/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNew verifies that New routes API calls through the /iceberg path prefix
// that Nessie requires. Full behavioral coverage lives in
// internal/catalog/rest/client_test.go.
func TestNew_ReturnsClient(t *testing.T) {
	mux := http.NewServeMux()
	// Nessie mounts the Iceberg REST API under /iceberg, so list-namespaces
	// lands at /iceberg/v1/namespaces rather than /v1/namespaces.
	mux.HandleFunc("/iceberg/v1/namespaces", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"namespaces":[]}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	c := nessie.New(config.CatalogConfig{URL: srv.URL})
	require.NotNil(t, c)

	ns, err := c.ListNamespaces(t.Context())
	require.NoError(t, err)
	assert.Empty(t, ns)
}

// TestNew_DefaultTokenEndpoint verifies that without a token_uri override the
// token endpoint is {url}/iceberg/oauth/tokens (Nessie default under the
// /iceberg prefix).
func TestNew_DefaultTokenEndpoint(t *testing.T) {
	tokenCalled := false
	mux := http.NewServeMux()
	mux.HandleFunc("/iceberg/oauth/tokens", func(w http.ResponseWriter, r *http.Request) {
		tokenCalled = true
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"access_token":"tok","expires_in":3600}`))
	})
	mux.HandleFunc("/iceberg/v1/namespaces", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"namespaces":[]}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	c := nessie.New(config.CatalogConfig{
		URL: srv.URL,
		Credential: config.CatalogCredential{
			ClientID:     "id",
			ClientSecret: "secret",
		},
	})
	_, err := c.ListNamespaces(t.Context())
	require.NoError(t, err)
	assert.True(t, tokenCalled, "default token endpoint {url}/iceberg/oauth/tokens should be used")
}

// TestNew_TokenURIOverride verifies that credential.token_uri replaces the
// default Nessie token endpoint — needed for Keycloak or other external IdPs.
func TestNew_TokenURIOverride(t *testing.T) {
	tokenCalled := false
	mux := http.NewServeMux()
	mux.HandleFunc("/keycloak/token", func(w http.ResponseWriter, r *http.Request) {
		tokenCalled = true
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"access_token":"tok","expires_in":3600}`))
	})
	mux.HandleFunc("/iceberg/v1/namespaces", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"namespaces":[]}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	c := nessie.New(config.CatalogConfig{
		URL: srv.URL,
		Credential: config.CatalogCredential{
			ClientID:     "id",
			ClientSecret: "secret",
			TokenURI:     srv.URL + "/keycloak/token",
		},
	})
	_, err := c.ListNamespaces(t.Context())
	require.NoError(t, err)
	assert.True(t, tokenCalled, "custom token URI should be used")
}
