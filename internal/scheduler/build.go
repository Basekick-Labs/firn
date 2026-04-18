package scheduler

import (
	"context"
	"fmt"

	"github.com/basekick-labs/firn/internal/catalog"
	"github.com/basekick-labs/firn/internal/catalog/glue"
	"github.com/basekick-labs/firn/internal/catalog/lakekeeper"
	"github.com/basekick-labs/firn/internal/config"
	"github.com/basekick-labs/firn/internal/storage"
	s3backend "github.com/basekick-labs/firn/internal/storage/s3"
)

func buildCatalog(cfg *config.Config) (catalog.Client, error) {
	switch cfg.Catalog.Type {
	case "lakekeeper":
		return lakekeeper.New(cfg.Catalog), nil
	case "glue":
		return glue.New(context.Background(), cfg.Catalog)
	default:
		return nil, fmt.Errorf("unsupported catalog type: %s", cfg.Catalog.Type)
	}
}

func buildStorage(cfg *config.Config) (storage.Backend, error) {
	switch cfg.Storage.Type {
	case "s3":
		// Bucket is derived from the catalog table locations at runtime;
		// the backend here is used for metadata reads (manifest files).
		return s3backend.New(context.Background(), cfg.Storage, "")
	default:
		return nil, fmt.Errorf("unsupported storage type: %s", cfg.Storage.Type)
	}
}
