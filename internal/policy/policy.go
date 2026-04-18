package policy

import (
	"strings"

	"github.com/basekick-labs/firn/internal/catalog"
	"github.com/basekick-labs/firn/internal/config"
)

// Resolver returns the effective maintenance policy for a table by merging
// table-level → namespace-level → global defaults (most specific wins).
type Resolver struct {
	cfg *config.MaintenanceConfig
}

func NewResolver(cfg *config.MaintenanceConfig) *Resolver {
	return &Resolver{cfg: cfg}
}

func (r *Resolver) For(id catalog.TableIdentifier) config.PolicyConfig {
	key := id.String()

	// Table-level override.
	if p, ok := r.cfg.Tables[key]; ok {
		return merge(r.cfg.Defaults, p)
	}

	// Namespace-level override.
	if p, ok := r.cfg.Namespaces[id.Namespace]; ok {
		return merge(r.cfg.Defaults, p)
	}

	// Also check bare namespace without trailing dot.
	ns := strings.TrimSuffix(id.Namespace, ".")
	if p, ok := r.cfg.Namespaces[ns]; ok {
		return merge(r.cfg.Defaults, p)
	}

	return r.cfg.Defaults
}

// merge applies overrides on top of base. Zero values in override are ignored
// so that partial namespace/table configs don't zero out defaults.
func merge(base, override config.PolicyConfig) config.PolicyConfig {
	result := base

	if override.Compaction.Strategy != "" {
		result.Compaction.Strategy = override.Compaction.Strategy
	}
	if override.Compaction.TargetFileSizeMB != 0 {
		result.Compaction.TargetFileSizeMB = override.Compaction.TargetFileSizeMB
	}
	if override.Compaction.MinFileCount != 0 {
		result.Compaction.MinFileCount = override.Compaction.MinFileCount
	}
	if override.Compaction.MinFileAgeMinutes != 0 {
		result.Compaction.MinFileAgeMinutes = override.Compaction.MinFileAgeMinutes
	}
	if len(override.Compaction.SortKeys) > 0 {
		result.Compaction.SortKeys = override.Compaction.SortKeys
	}
	if len(override.Compaction.ZOrderColumns) > 0 {
		result.Compaction.ZOrderColumns = override.Compaction.ZOrderColumns
	}
	if override.Compaction.Enabled {
		result.Compaction.Enabled = true
	}

	if override.SnapshotExpiry.MinSnapshotsToKeep != 0 {
		result.SnapshotExpiry.MinSnapshotsToKeep = override.SnapshotExpiry.MinSnapshotsToKeep
	}
	if override.SnapshotExpiry.MaxSnapshotAgeHours != 0 {
		result.SnapshotExpiry.MaxSnapshotAgeHours = override.SnapshotExpiry.MaxSnapshotAgeHours
	}
	if override.SnapshotExpiry.Enabled {
		result.SnapshotExpiry.Enabled = true
	}

	if override.OrphanCleanup.GracePeriodHours != 0 {
		result.OrphanCleanup.GracePeriodHours = override.OrphanCleanup.GracePeriodHours
	}
	if override.OrphanCleanup.Enabled {
		result.OrphanCleanup.Enabled = true
	}

	return result
}
