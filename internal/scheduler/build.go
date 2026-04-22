package scheduler

import (
	"context"
	"fmt"

	"github.com/basekick-labs/firn/internal/catalog"
	"github.com/basekick-labs/firn/internal/catalog/glue"
	"github.com/basekick-labs/firn/internal/catalog/lakekeeper"
	"github.com/basekick-labs/firn/internal/catalog/nessie"
	"github.com/basekick-labs/firn/internal/catalog/polaris"
	"github.com/basekick-labs/firn/internal/config"
	"github.com/basekick-labs/firn/internal/storage"
	azurebackend "github.com/basekick-labs/firn/internal/storage/azure"
	gcsbackend "github.com/basekick-labs/firn/internal/storage/gcs"
	s3backend "github.com/basekick-labs/firn/internal/storage/s3"
)

func buildCatalog(cfg *config.Config) (catalog.Client, error) {
	switch cfg.Catalog.Type {
	case "lakekeeper":
		return lakekeeper.New(cfg.Catalog), nil
	case "polaris":
		return polaris.New(cfg.Catalog), nil
	case "nessie":
		return nessie.New(cfg.Catalog), nil
	case "glue":
		return glue.New(context.Background(), cfg.Catalog)
	default:
		return nil, fmt.Errorf("unsupported catalog type: %s", cfg.Catalog.Type)
	}
}

func buildStorage(cfg *config.Config) (storage.Backend, error) {
	// Bucket/container is derived from catalog table locations at runtime;
	// the backend here is used for metadata reads (manifest files) and passes
	// an empty bucket — the concrete backends populate it per-table.
	switch cfg.Storage.Type {
	case "s3":
		return s3backend.New(context.Background(), cfg.Storage, "")
	case "gcs":
		return gcsbackend.New(context.Background(), cfg.Storage, "")
	case "azure":
		return azurebackend.New(context.Background(), cfg.Storage, "")
	default:
		return nil, fmt.Errorf("unsupported storage type: %s", cfg.Storage.Type)
	}
}
