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

func TestLoad_StorageTypeGCS(t *testing.T) {
	path := writeTemp(t, `
catalog:
  type: lakekeeper
  url: http://localhost:8080
storage:
  type: gcs
  project: my-project
  credentials_json: '{"type":"service_account"}'
`)
	cfg, err := Load(path)
	require.NoError(t, err)
	assert.Equal(t, "gcs", cfg.Storage.Type)
	assert.Equal(t, "my-project", cfg.Storage.Project)
}

func TestLoad_StorageTypeAzure(t *testing.T) {
	path := writeTemp(t, `
catalog:
  type: lakekeeper
  url: http://localhost:8080
storage:
  type: azure
  account: myaccount
  container: mycontainer
  account_key: mykey
`)
	cfg, err := Load(path)
	require.NoError(t, err)
	assert.Equal(t, "azure", cfg.Storage.Type)
	assert.Equal(t, "myaccount", cfg.Storage.Account)
	assert.Equal(t, "mycontainer", cfg.Storage.Container)
}

func TestLoad_StorageTypeAzureMissingContainer(t *testing.T) {
	path := writeTemp(t, `
catalog:
  type: lakekeeper
  url: http://localhost:8080
storage:
  type: azure
  account: myaccount
  account_key: mykey
`)
	_, err := Load(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "storage.container")
}

func TestLoad_StorageTypeUnsupported(t *testing.T) {
	path := writeTemp(t, `
catalog:
  type: lakekeeper
  url: http://localhost:8080
storage:
  type: hdfs
`)
	_, err := Load(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "hdfs")
}

func TestLoad_RetryDefaults(t *testing.T) {
	path := writeTemp(t, `
catalog:
  type: lakekeeper
  url: http://localhost:8080
storage:
  type: s3
`)
	cfg, err := Load(path)
	require.NoError(t, err)
	assert.Equal(t, 5, cfg.Scheduler.Retry.MaxAttempts)
	assert.Equal(t, "200ms", cfg.Scheduler.Retry.BaseDelay)
	assert.Equal(t, "10s", cfg.Scheduler.Retry.MaxDelay)
}

func TestLoad_RetryExplicit(t *testing.T) {
	path := writeTemp(t, `
catalog:
  type: lakekeeper
  url: http://localhost:8080
storage:
  type: s3
scheduler:
  retry:
    max_attempts: 10
    base_delay: 500ms
    max_delay: 30s
`)
	cfg, err := Load(path)
	require.NoError(t, err)
	assert.Equal(t, 10, cfg.Scheduler.Retry.MaxAttempts)
	assert.Equal(t, "500ms", cfg.Scheduler.Retry.BaseDelay)
	assert.Equal(t, "30s", cfg.Scheduler.Retry.MaxDelay)
}

func TestLoad_RetryInvalidBaseDelay(t *testing.T) {
	path := writeTemp(t, `
catalog:
  type: lakekeeper
  url: http://localhost:8080
storage:
  type: s3
scheduler:
  retry:
    base_delay: not-a-duration
`)
	_, err := Load(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "base_delay")
}

func TestLoad_RetryInvalidMaxDelay(t *testing.T) {
	path := writeTemp(t, `
catalog:
  type: lakekeeper
  url: http://localhost:8080
storage:
  type: s3
scheduler:
  retry:
    max_delay: bad
`)
	_, err := Load(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max_delay")
}

func TestLoad_RetryMaxAttemptsZeroIsDefaulted(t *testing.T) {
	path := writeTemp(t, `
catalog:
  type: lakekeeper
  url: http://localhost:8080
storage:
  type: s3
scheduler:
  retry:
    max_attempts: 0
`)
	cfg, err := Load(path)
	require.NoError(t, err)
	assert.Equal(t, 5, cfg.Scheduler.Retry.MaxAttempts)
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
