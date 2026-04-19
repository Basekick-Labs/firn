package orphan

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/basekick-labs/firn/internal/catalog"
	"github.com/basekick-labs/firn/internal/config"
	"github.com/basekick-labs/firn/internal/iceberg"
	"github.com/basekick-labs/firn/internal/storage"
	"github.com/rs/zerolog/log"
)

// Engine drives orphan file cleanup for a single table.
type Engine struct {
	catalog catalog.Client
	storage storage.Backend
}

// NewEngine creates a new orphan cleanup Engine.
func NewEngine(cat catalog.Client, stor storage.Backend) *Engine {
	return &Engine{catalog: cat, storage: stor}
}

// Result is the outcome of one orphan cleanup run against a table.
type Result struct {
	Table        catalog.TableIdentifier
	ScannedFiles int
	DeletedFiles int
	SkippedFiles int // within grace period
	Duration     time.Duration
}

// ExecuteCleanup scans all files under the table location and deletes any that
// are not referenced by the current Iceberg metadata and are older than the
// configured grace period.
func (e *Engine) ExecuteCleanup(ctx context.Context, id catalog.TableIdentifier, policy config.OrphanCleanupPolicy) (Result, error) {
	if !policy.Enabled {
		return Result{Table: id}, nil
	}

	start := time.Now()

	meta, err := e.catalog.LoadTable(ctx, id)
	if err != nil {
		return Result{}, fmt.Errorf("load table: %w", err)
	}

	liveFiles, err := e.collectLiveFiles(ctx, meta)
	if err != nil {
		return Result{}, fmt.Errorf("collect live files: %w", err)
	}

	prefix, err := iceberg.URIToPath(meta.Location)
	if err != nil {
		return Result{}, fmt.Errorf("parse table location %s: %w", meta.Location, err)
	}

	keys, err := e.storage.List(ctx, prefix)
	if err != nil {
		return Result{}, fmt.Errorf("list storage prefix %s: %w", prefix, err)
	}

	cutoff := time.Now().Add(-time.Duration(policy.GracePeriodHours) * time.Hour)

	var scanned, deleted, skipped int
	for _, key := range keys {
		scanned++

		if liveFiles[key] {
			continue
		}

		// Never delete catalog-managed metadata JSON files — they are not
		// reachable via the snapshot→manifest→data chain but must be preserved.
		if strings.HasSuffix(key, ".json") {
			continue
		}

		modTime, err := e.storage.ModTime(ctx, key)
		if err != nil {
			log.Warn().Err(err).Str("path", key).Msg("cannot determine file age, skipping")
			skipped++
			continue
		}

		if modTime.After(cutoff) {
			skipped++
			continue
		}

		if err := e.storage.Delete(ctx, key); err != nil {
			log.Warn().Err(err).Str("path", key).Msg("failed to delete orphan file")
		} else {
			deleted++
		}
	}

	return Result{
		Table:        id,
		ScannedFiles: scanned,
		DeletedFiles: deleted,
		SkippedFiles: skipped,
		Duration:     time.Since(start),
	}, nil
}

// collectLiveFiles returns the set of storage keys that are referenced by the
// current table metadata. Any file not in this set is an orphan candidate.
//
// If any manifest list or manifest cannot be read the function returns an error
// and the caller must not delete any files.
func (e *Engine) collectLiveFiles(ctx context.Context, meta *iceberg.TableMetadata) (map[string]bool, error) {
	live := make(map[string]bool)

	for i := range meta.Snapshots {
		s := &meta.Snapshots[i]
		if s.ManifestList == "" {
			continue
		}

		mlPath, err := iceberg.URIToPath(s.ManifestList)
		if err != nil {
			return nil, fmt.Errorf("parse manifest list URI %s: %w", s.ManifestList, err)
		}
		live[mlPath] = true

		mfs, err := iceberg.ReadManifestListAll(ctx, e.storage, s.ManifestList)
		if err != nil {
			return nil, fmt.Errorf("read manifest list %s: %w", s.ManifestList, err)
		}

		for _, mf := range mfs {
			mPath, err := iceberg.URIToPath(mf.Path)
			if err != nil {
				return nil, fmt.Errorf("parse manifest URI %s: %w", mf.Path, err)
			}
			live[mPath] = true

			entries, err := iceberg.ReadManifest(ctx, e.storage, mf.Path)
			if err != nil {
				return nil, fmt.Errorf("read manifest %s: %w", mf.Path, err)
			}

			for _, entry := range entries {
				dPath, err := iceberg.URIToPath(entry.DataFile.FilePath)
				if err != nil {
					log.Warn().Err(err).Str("uri", entry.DataFile.FilePath).Msg("cannot parse data file URI, treating as live")
					continue
				}
				live[dPath] = true
			}
		}
	}

	return live, nil
}
