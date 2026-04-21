// Package polaris provides an Iceberg catalog client for Apache Polaris.
// Polaris speaks the standard Iceberg REST spec at /v1/... and uses OAuth2
// client credentials with its token endpoint at {url}/oauth/tokens.
package polaris

import (
	"strings"

	"github.com/basekick-labs/firn/internal/catalog/rest"
	"github.com/basekick-labs/firn/internal/config"
)

// New returns a catalog client for an Apache Polaris instance.
func New(cfg config.CatalogConfig) *rest.Client {
	base := strings.TrimRight(cfg.URL, "/")
	tokenEndpoint := base + "/oauth/tokens"
	if cfg.Credential.TokenURI != "" {
		tokenEndpoint = cfg.Credential.TokenURI
	}
	return rest.NewClient(base, tokenEndpoint, cfg.Credential)
}
