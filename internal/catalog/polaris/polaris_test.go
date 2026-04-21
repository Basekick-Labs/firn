package polaris_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/basekick-labs/firn/internal/catalog/polaris"
	"github.com/basekick-labs/firn/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNew verifies that New wires the standard Polaris base URL and token
// endpoint correctly. Full behavioral coverage lives in
// internal/catalog/rest/client_test.go.
func TestNew_ReturnsClient(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/namespaces", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"namespaces":[]}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	c := polaris.New(config.CatalogConfig{URL: srv.URL})
	require.NotNil(t, c)

	ns, err := c.ListNamespaces(t.Context())
	require.NoError(t, err)
	assert.Empty(t, ns)
}

// TestNew_DefaultTokenEndpoint verifies that without a token_uri override the
// token endpoint is constructed as {url}/oauth/tokens (Polaris default).
func TestNew_DefaultTokenEndpoint(t *testing.T) {
	tokenCalled := false
	mux := http.NewServeMux()
	mux.HandleFunc("/oauth/tokens", func(w http.ResponseWriter, r *http.Request) {
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

	c := polaris.New(config.CatalogConfig{
		URL: srv.URL,
		Credential: config.CatalogCredential{
			ClientID:     "id",
			ClientSecret: "secret",
		},
	})
	_, err := c.ListNamespaces(t.Context())
	require.NoError(t, err)
	assert.True(t, tokenCalled, "default token endpoint {url}/oauth/tokens should be used")
}

// TestNew_TokenURIOverride verifies that credential.token_uri replaces the
// default Polaris token endpoint.
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

	c := polaris.New(config.CatalogConfig{
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
