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

// mergeEnabled returns override if explicitly set (non-nil), otherwise base.
func mergeEnabled(base, override *bool) *bool {
	if override != nil {
		return override
	}
	return base
}

// merge applies overrides on top of base. Zero/nil values in override are ignored
// so that partial namespace/table configs don't zero out defaults.
func merge(base, override config.PolicyConfig) config.PolicyConfig {
	result := base

	result.Compaction.Enabled = mergeEnabled(base.Compaction.Enabled, override.Compaction.Enabled)
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

	result.SnapshotExpiry.Enabled = mergeEnabled(base.SnapshotExpiry.Enabled, override.SnapshotExpiry.Enabled)
	if override.SnapshotExpiry.MinSnapshotsToKeep != 0 {
		result.SnapshotExpiry.MinSnapshotsToKeep = override.SnapshotExpiry.MinSnapshotsToKeep
	}
	if override.SnapshotExpiry.MaxSnapshotAgeHours != 0 {
		result.SnapshotExpiry.MaxSnapshotAgeHours = override.SnapshotExpiry.MaxSnapshotAgeHours
	}

	result.OrphanCleanup.Enabled = mergeEnabled(base.OrphanCleanup.Enabled, override.OrphanCleanup.Enabled)
	if override.OrphanCleanup.GracePeriodHours != 0 {
		result.OrphanCleanup.GracePeriodHours = override.OrphanCleanup.GracePeriodHours
	}

	return result
}
