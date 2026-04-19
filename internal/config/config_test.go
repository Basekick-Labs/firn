package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "firn-*.yaml")
	require.NoError(t, err)
	_, err = f.WriteString(content)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return f.Name()
}

func TestLoad_ExplicitEnabledFalseInDefaults(t *testing.T) {
	path := writeTemp(t, `
catalog:
  type: lakekeeper
  url: http://localhost:8080
storage:
  type: s3
maintenance:
  defaults:
    compaction:
      enabled: false
    snapshot_expiry:
      enabled: false
    orphan_cleanup:
      enabled: false
`)
	cfg, err := Load(path)
	require.NoError(t, err)

	assert.False(t, cfg.Maintenance.Defaults.Compaction.IsEnabled(), "explicit enabled: false must be preserved")
	assert.False(t, cfg.Maintenance.Defaults.SnapshotExpiry.IsEnabled())
	assert.False(t, cfg.Maintenance.Defaults.OrphanCleanup.IsEnabled())
}

func TestLoad_ExplicitEnabledTrueInDefaults(t *testing.T) {
	path := writeTemp(t, `
catalog:
  type: lakekeeper
  url: http://localhost:8080
storage:
  type: s3
maintenance:
  defaults:
    compaction:
      enabled: true
    snapshot_expiry:
      enabled: true
    orphan_cleanup:
      enabled: true
`)
	cfg, err := Load(path)
	require.NoError(t, err)

	assert.True(t, cfg.Maintenance.Defaults.Compaction.IsEnabled())
	assert.True(t, cfg.Maintenance.Defaults.SnapshotExpiry.IsEnabled())
	assert.True(t, cfg.Maintenance.Defaults.OrphanCleanup.IsEnabled())
}

func TestLoad_EnabledAbsentInDefaults_DefaultsToFalse(t *testing.T) {
	// When enabled is not specified at all, applyDefaults sets it to false.
	path := writeTemp(t, `
catalog:
  type: lakekeeper
  url: http://localhost:8080
storage:
  type: s3
`)
	cfg, err := Load(path)
	require.NoError(t, err)

	assert.False(t, cfg.Maintenance.Defaults.Compaction.IsEnabled())
	assert.False(t, cfg.Maintenance.Defaults.SnapshotExpiry.IsEnabled())
	assert.False(t, cfg.Maintenance.Defaults.OrphanCleanup.IsEnabled())
}

func TestLoad_NamespaceOverrideEnabledFalse(t *testing.T) {
	path := writeTemp(t, `
catalog:
  type: lakekeeper
  url: http://localhost:8080
storage:
  type: s3
maintenance:
  defaults:
    compaction:
      enabled: true
  namespaces:
    archive:
      compaction:
        enabled: false
`)
	cfg, err := Load(path)
	require.NoError(t, err)

	nsPolicy, ok := cfg.Maintenance.Namespaces["archive"]
	require.True(t, ok)
	// The raw namespace override should have Enabled = &false (not nil).
	require.NotNil(t, nsPolicy.Compaction.Enabled)
	assert.False(t, *nsPolicy.Compaction.Enabled)
}
