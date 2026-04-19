package policy

import (
	"testing"

	"github.com/basekick-labs/firn/internal/catalog"
	"github.com/basekick-labs/firn/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func boolPtr(b bool) *bool { return &b }

func defaultPolicy() config.PolicyConfig {
	return config.PolicyConfig{
		Compaction: config.CompactionPolicy{
			Enabled:           boolPtr(true),
			Strategy:          "binpack",
			TargetFileSizeMB:  512,
			MinFileCount:      5,
			MinFileAgeMinutes: 60,
		},
		SnapshotExpiry: config.SnapshotExpiry{
			Enabled:             boolPtr(true),
			MinSnapshotsToKeep:  5,
			MaxSnapshotAgeHours: 120,
		},
		OrphanCleanup: config.OrphanCleanupPolicy{
			Enabled:          boolPtr(true),
			GracePeriodHours: 24,
		},
	}
}

func makeResolver(defaults config.PolicyConfig, namespaces map[string]config.PolicyConfig, tables map[string]config.PolicyConfig) *Resolver {
	return NewResolver(&config.MaintenanceConfig{
		Defaults:   defaults,
		Namespaces: namespaces,
		Tables:     tables,
	})
}

func TestResolverFor_NoOverride(t *testing.T) {
	r := makeResolver(defaultPolicy(), nil, nil)
	p := r.For(catalog.TableIdentifier{Namespace: "ns", Name: "tbl"})
	assert.True(t, p.Compaction.IsEnabled())
	assert.Equal(t, "binpack", p.Compaction.Strategy)
	assert.Equal(t, 512, p.Compaction.TargetFileSizeMB)
	assert.True(t, p.SnapshotExpiry.IsEnabled())
	assert.True(t, p.OrphanCleanup.IsEnabled())
}

func TestResolverFor_NamespaceOverride_PartialFields(t *testing.T) {
	r := makeResolver(defaultPolicy(), map[string]config.PolicyConfig{
		"ns": {
			Compaction: config.CompactionPolicy{
				Strategy:         "sort",
				SortKeys:         []string{"ts"},
				TargetFileSizeMB: 1024,
			},
		},
	}, nil)

	p := r.For(catalog.TableIdentifier{Namespace: "ns", Name: "tbl"})
	assert.True(t, p.Compaction.IsEnabled(), "enabled should inherit from defaults")
	assert.Equal(t, "sort", p.Compaction.Strategy)
	assert.Equal(t, []string{"ts"}, p.Compaction.SortKeys)
	assert.Equal(t, 1024, p.Compaction.TargetFileSizeMB)
	assert.Equal(t, 5, p.Compaction.MinFileCount, "unset fields inherit from defaults")
	assert.True(t, p.SnapshotExpiry.IsEnabled(), "snapshot expiry unaffected by compaction override")
}

func TestResolverFor_NamespaceOverride_DisableCompaction(t *testing.T) {
	r := makeResolver(defaultPolicy(), map[string]config.PolicyConfig{
		"analytics": {
			Compaction: config.CompactionPolicy{
				Enabled: boolPtr(false),
			},
		},
	}, nil)

	p := r.For(catalog.TableIdentifier{Namespace: "analytics", Name: "events"})
	assert.False(t, p.Compaction.IsEnabled(), "explicit enabled: false must disable compaction")
	assert.True(t, p.SnapshotExpiry.IsEnabled(), "other policies unaffected")
}

func TestResolverFor_TableOverride_WinsOverNamespace(t *testing.T) {
	r := makeResolver(defaultPolicy(), map[string]config.PolicyConfig{
		"ns": {
			Compaction: config.CompactionPolicy{Strategy: "sort", SortKeys: []string{"ts"}},
		},
	}, map[string]config.PolicyConfig{
		"ns.tbl": {
			Compaction: config.CompactionPolicy{Strategy: "binpack"},
		},
	})

	p := r.For(catalog.TableIdentifier{Namespace: "ns", Name: "tbl"})
	assert.Equal(t, "binpack", p.Compaction.Strategy, "table override wins over namespace")
	assert.Nil(t, p.Compaction.SortKeys, "table override merges against defaults, not namespace; namespace sort_keys not inherited")
	assert.Equal(t, 5, p.Compaction.MinFileCount, "numeric defaults still inherited from global defaults")
}

func TestResolverFor_TableOverride_DisableExpiry(t *testing.T) {
	r := makeResolver(defaultPolicy(), nil, map[string]config.PolicyConfig{
		"ns.sensitive": {
			SnapshotExpiry: config.SnapshotExpiry{Enabled: boolPtr(false)},
			OrphanCleanup:  config.OrphanCleanupPolicy{Enabled: boolPtr(false)},
		},
	})

	p := r.For(catalog.TableIdentifier{Namespace: "ns", Name: "sensitive"})
	assert.True(t, p.Compaction.IsEnabled(), "compaction unaffected")
	assert.False(t, p.SnapshotExpiry.IsEnabled())
	assert.False(t, p.OrphanCleanup.IsEnabled())
}

func TestResolverFor_NamespaceTrailingDot(t *testing.T) {
	r := makeResolver(defaultPolicy(), map[string]config.PolicyConfig{
		"ns": {
			Compaction: config.CompactionPolicy{TargetFileSizeMB: 256},
		},
	}, nil)

	// Namespace with trailing dot should still match.
	p := r.For(catalog.TableIdentifier{Namespace: "ns.", Name: "tbl"})
	assert.Equal(t, 256, p.Compaction.TargetFileSizeMB)
}

func TestResolverFor_UnknownNamespaceAndTable(t *testing.T) {
	r := makeResolver(defaultPolicy(), map[string]config.PolicyConfig{
		"other": {},
	}, map[string]config.PolicyConfig{
		"other.tbl": {},
	})

	p := r.For(catalog.TableIdentifier{Namespace: "ns", Name: "tbl"})
	assert.Equal(t, defaultPolicy(), p, "unmatched table/namespace returns defaults unchanged")
}

func TestResolverFor_SnapshotExpiryNumericOverride(t *testing.T) {
	r := makeResolver(defaultPolicy(), map[string]config.PolicyConfig{
		"ns": {
			SnapshotExpiry: config.SnapshotExpiry{
				MinSnapshotsToKeep:  10,
				MaxSnapshotAgeHours: 48,
			},
		},
	}, nil)

	p := r.For(catalog.TableIdentifier{Namespace: "ns", Name: "tbl"})
	require.True(t, p.SnapshotExpiry.IsEnabled())
	assert.Equal(t, 10, p.SnapshotExpiry.MinSnapshotsToKeep)
	assert.Equal(t, 48, p.SnapshotExpiry.MaxSnapshotAgeHours)
}

func TestResolverFor_OrphanGracePeriodOverride(t *testing.T) {
	r := makeResolver(defaultPolicy(), map[string]config.PolicyConfig{
		"ns": {
			OrphanCleanup: config.OrphanCleanupPolicy{GracePeriodHours: 72},
		},
	}, nil)

	p := r.For(catalog.TableIdentifier{Namespace: "ns", Name: "tbl"})
	assert.True(t, p.OrphanCleanup.IsEnabled())
	assert.Equal(t, 72, p.OrphanCleanup.GracePeriodHours)
}
