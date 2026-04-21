// Package nessie provides an Iceberg catalog client for Project Nessie.
// Nessie exposes the Iceberg REST spec under the /iceberg path prefix, so all
// API calls become /iceberg/v1/namespaces/... etc. The OAuth2 token endpoint
// defaults to {url}/oauth/tokens but can be overridden via credential.token_uri
// for Keycloak or other external IdPs.
package nessie

import (
	"strings"

	"github.com/basekick-labs/firn/internal/catalog/rest"
	"github.com/basekick-labs/firn/internal/config"
)

// New returns a catalog client for a Project Nessie instance.
func New(cfg config.CatalogConfig) *rest.Client {
	base := strings.TrimRight(cfg.URL, "/") + "/iceberg"
	tokenEndpoint := base + "/oauth/tokens"
	if cfg.Credential.TokenURI != "" {
		tokenEndpoint = cfg.Credential.TokenURI
	}
	return rest.NewClient(base, tokenEndpoint, cfg.Credential)
}
