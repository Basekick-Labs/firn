package compaction

import (
	"context"
	"time"

	"github.com/basekick-labs/firn/internal/catalog"
	"github.com/basekick-labs/firn/internal/config"
	"github.com/basekick-labs/firn/internal/iceberg"
	"github.com/basekick-labs/firn/internal/storage"
)

// Candidate is a group of files in the same partition that should be merged.
type Candidate struct {
	Table     catalog.TableIdentifier
	Partition string
	Files     []DataFileInfo
	Policy    config.CompactionPolicy
}

// DataFileInfo holds the fields from an Iceberg manifest entry needed for
// candidate selection.
type DataFileInfo struct {
	Path      string
	SizeBytes int64
	AddedAt   time.Time
}

// Result is the outcome of a single compaction job.
type Result struct {
	Table          catalog.TableIdentifier
	Partition      string
	InputFiles     []string
	OutputFile     string
	OutputSize     int64
	BytesBefore    int64
	BytesAfter     int64
	Duration       time.Duration
}

// SubprocessConfig is serialized to stdin of the compact subprocess.
type SubprocessConfig struct {
	JobID         string              `json:"job_id"`
	InputFiles    []string            `json:"input_files"`
	OutputPath    string              `json:"output_path"`
	SortKeys      []string            `json:"sort_keys"`
	Strategy      string              `json:"strategy"`
	MemoryLimit   string              `json:"memory_limit"`
	StorageType   string              `json:"storage_type"`
	StorageConfig string              `json:"storage_config"` // JSON-encoded backend config
	TempDir       string              `json:"temp_dir"`
}

// SubprocessResult is serialized from stdout of the compact subprocess.
type SubprocessResult struct {
	Success     bool   `json:"success"`
	Error       string `json:"error,omitempty"`
	OutputSize  int64  `json:"output_size"`
	BytesBefore int64  `json:"bytes_before"`
}

// Engine selects compaction candidates from Iceberg metadata and executes jobs.
type Engine struct {
	catalog catalog.Client
	storage storage.Backend
	cfg     *config.Config
}

func NewEngine(cat catalog.Client, stor storage.Backend, cfg *config.Config) *Engine {
	return &Engine{catalog: cat, storage: stor, cfg: cfg}
}

// FindCandidates reads the current snapshot's manifests and returns file groups
// eligible for compaction according to the given policy.
func (e *Engine) FindCandidates(ctx context.Context, id catalog.TableIdentifier, policy config.CompactionPolicy) ([]Candidate, error) {
	meta, err := e.catalog.LoadTable(ctx, id)
	if err != nil {
		return nil, err
	}

	snap := meta.CurrentSnapshot()
	if snap == nil {
		return nil, nil
	}

	files, err := e.listDataFiles(ctx, meta, snap)
	if err != nil {
		return nil, err
	}

	return selectCandidates(id, files, policy), nil
}

// listDataFiles walks manifest list → manifests → data files for a snapshot.
func (e *Engine) listDataFiles(ctx context.Context, meta *iceberg.TableMetadata, snap *iceberg.Snapshot) ([]DataFileInfo, error) {
	// TODO: read Avro manifest list from snap.ManifestList via storage,
	// walk each ManifestFile, read data files from each manifest.
	// Placeholder until Avro manifest reading is implemented.
	_ = ctx
	_ = meta
	_ = snap
	return nil, nil
}

// selectCandidates groups files by partition and applies policy filters.
func selectCandidates(id catalog.TableIdentifier, files []DataFileInfo, policy config.CompactionPolicy) []Candidate {
	if !policy.Enabled {
		return nil
	}

	// Group by partition (placeholder: single partition until manifest parsing is done).
	if len(files) < policy.MinFileCount {
		return nil
	}

	minAge := time.Duration(policy.MinFileAgeMinutes) * time.Minute
	cutoff := time.Now().UTC().Add(-minAge)

	var eligible []DataFileInfo
	for _, f := range files {
		if f.AddedAt.Before(cutoff) {
			eligible = append(eligible, f)
		}
	}

	if len(eligible) < policy.MinFileCount {
		return nil
	}

	return []Candidate{{
		Table:  id,
		Files:  eligible,
		Policy: policy,
	}}
}

// RunSubprocess is the entrypoint called by cmd/compact.
// It executes the DuckDB compaction job in-process (this IS the subprocess).
func RunSubprocess(cfg SubprocessConfig) SubprocessResult {
	// TODO: initialize storage backend from cfg.StorageConfig,
	// download input files, run DuckDB COPY query, upload output.
	_ = cfg
	return SubprocessResult{Success: false, Error: "not yet implemented"}
}
