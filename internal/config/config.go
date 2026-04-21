package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Debug       bool              `yaml:"debug"`
	Catalog     CatalogConfig     `yaml:"catalog"`
	Storage     StorageConfig     `yaml:"storage"`
	Maintenance MaintenanceConfig `yaml:"maintenance"`
	Scheduler   SchedulerConfig   `yaml:"scheduler"`
}

type CatalogConfig struct {
	Type       string            `yaml:"type"`   // lakekeeper | polaris | nessie | glue
	URL        string            `yaml:"url"`    // for REST catalogs
	Region     string            `yaml:"region"` // for glue
	Credential CatalogCredential `yaml:"credential"`
}

type CatalogCredential struct {
	ClientID     string `yaml:"client_id"`
	ClientSecret string `yaml:"client_secret"`
	TokenURI     string `yaml:"token_uri"` // optional: override OAuth2 token endpoint
}

type StorageConfig struct {
	Type            string `yaml:"type"` // s3
	Endpoint        string `yaml:"endpoint"`
	Region          string `yaml:"region"`
	AccessKeyID     string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`
	PathStyle       bool   `yaml:"path_style"`
}

type MaintenanceConfig struct {
	Defaults   PolicyConfig            `yaml:"defaults"`
	Namespaces map[string]PolicyConfig `yaml:"namespaces"`
	Tables     map[string]PolicyConfig `yaml:"tables"`
}

type PolicyConfig struct {
	Compaction     CompactionPolicy    `yaml:"compaction"`
	SnapshotExpiry SnapshotExpiry      `yaml:"snapshot_expiry"`
	OrphanCleanup  OrphanCleanupPolicy `yaml:"orphan_cleanup"`
}

type CompactionPolicy struct {
	// Enabled uses *bool so that absent YAML field (nil) is distinguishable from
	// explicit "enabled: false", allowing overrides to disable a default-enabled policy.
	Enabled           *bool    `yaml:"enabled"`
	Strategy          string   `yaml:"strategy"` // binpack | sort | z-order
	TargetFileSizeMB  int      `yaml:"target_file_size_mb"`
	MinFileCount      int      `yaml:"min_file_count"`
	MinFileAgeMinutes int      `yaml:"min_file_age_minutes"`
	SortKeys          []string `yaml:"sort_keys"`
	ZOrderColumns     []string `yaml:"z_order_columns"`
}

// IsEnabled reports whether compaction is enabled. Returns false if Enabled is nil.
func (p CompactionPolicy) IsEnabled() bool { return p.Enabled != nil && *p.Enabled }

type SnapshotExpiry struct {
	Enabled             *bool `yaml:"enabled"`
	MinSnapshotsToKeep  int   `yaml:"min_snapshots_to_keep"`
	MaxSnapshotAgeHours int   `yaml:"max_snapshot_age_hours"`
}

// IsEnabled reports whether snapshot expiry is enabled. Returns false if Enabled is nil.
func (p SnapshotExpiry) IsEnabled() bool { return p.Enabled != nil && *p.Enabled }

type OrphanCleanupPolicy struct {
	Enabled          *bool `yaml:"enabled"`
	GracePeriodHours int   `yaml:"grace_period_hours"`
}

// IsEnabled reports whether orphan cleanup is enabled. Returns false if Enabled is nil.
func (p OrphanCleanupPolicy) IsEnabled() bool { return p.Enabled != nil && *p.Enabled }

type SchedulerConfig struct {
	Interval          string `yaml:"interval"`
	MaxConcurrentJobs int    `yaml:"max_concurrent_jobs"`
	MemoryLimit       string `yaml:"memory_limit"`
	MetricsAddr       string `yaml:"metrics_addr"` // HTTP address for /metrics and /healthz; empty disables
}

func Load(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config %s: %w", path, err)
	}
	defer f.Close()

	var cfg Config
	if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	applyDefaults(&cfg)

	if err := validate(&cfg); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &cfg, nil
}

func boolPtr(b bool) *bool { return &b }

func applyDefaults(cfg *Config) {
	if cfg.Scheduler.Interval == "" {
		cfg.Scheduler.Interval = "5m"
	}
	if cfg.Scheduler.MaxConcurrentJobs == 0 {
		cfg.Scheduler.MaxConcurrentJobs = 4
	}
	if cfg.Scheduler.MemoryLimit == "" {
		cfg.Scheduler.MemoryLimit = "4GB"
	}

	d := &cfg.Maintenance.Defaults
	if d.Compaction.Enabled == nil {
		d.Compaction.Enabled = boolPtr(false)
	}
	if d.Compaction.TargetFileSizeMB == 0 {
		d.Compaction.TargetFileSizeMB = 512
	}
	if d.Compaction.MinFileCount == 0 {
		d.Compaction.MinFileCount = 5
	}
	if d.Compaction.MinFileAgeMinutes == 0 {
		d.Compaction.MinFileAgeMinutes = 60
	}
	if d.SnapshotExpiry.Enabled == nil {
		d.SnapshotExpiry.Enabled = boolPtr(false)
	}
	if d.SnapshotExpiry.MinSnapshotsToKeep == 0 {
		d.SnapshotExpiry.MinSnapshotsToKeep = 5
	}
	if d.SnapshotExpiry.MaxSnapshotAgeHours == 0 {
		d.SnapshotExpiry.MaxSnapshotAgeHours = 120
	}
	if d.OrphanCleanup.Enabled == nil {
		d.OrphanCleanup.Enabled = boolPtr(false)
	}
	if d.OrphanCleanup.GracePeriodHours == 0 {
		d.OrphanCleanup.GracePeriodHours = 24
	}
}

func validate(cfg *Config) error {
	if cfg.Catalog.Type == "" {
		return fmt.Errorf("catalog.type is required")
	}
	switch cfg.Catalog.Type {
	case "lakekeeper", "polaris", "nessie", "glue":
	default:
		return fmt.Errorf("catalog.type %q is not supported", cfg.Catalog.Type)
	}
	if cfg.Storage.Type == "" {
		return fmt.Errorf("storage.type is required")
	}
	return nil
}
