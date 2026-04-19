package compaction

import (
	"context"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/basekick-labs/firn/internal/catalog"
	"github.com/basekick-labs/firn/internal/config"
	"github.com/basekick-labs/firn/internal/iceberg"
	"github.com/basekick-labs/firn/internal/storage"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

// Candidate is a group of files in the same partition ready to be merged.
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
	Table       catalog.TableIdentifier
	Partition   string
	InputFiles  []string
	OutputFile  string
	OutputSize  int64
	BytesBefore int64
	BytesAfter  int64
	Duration    time.Duration
}

// SubprocessConfig is serialized to stdin of the compact subprocess.
type SubprocessConfig struct {
	JobID         string   `json:"job_id"`
	InputFiles    []string `json:"input_files"`
	OutputPath    string   `json:"output_path"`
	SortKeys      []string `json:"sort_keys"`
	Strategy      string   `json:"strategy"`
	MemoryLimit   string   `json:"memory_limit"`
	StorageType   string   `json:"storage_type"`
	StorageConfig string   `json:"storage_config"`
	TempDir       string   `json:"temp_dir"`
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
	if policy.Strategy == "sort" && len(policy.SortKeys) == 0 {
		return nil, fmt.Errorf("sort compaction strategy requires at least one sort_key")
	}

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

// listDataFiles walks the manifest list → manifests → data file entries for
// the given snapshot and returns one DataFileInfo per live data file.
func (e *Engine) listDataFiles(ctx context.Context, meta *iceberg.TableMetadata, snap *iceberg.Snapshot) ([]DataFileInfo, error) {
	manifests, err := iceberg.ReadManifestList(ctx, e.storage, snap.ManifestList)
	if err != nil {
		return nil, err
	}

	// Build snapshot ID → timestamp map for age lookup.
	snapTimes := make(map[int64]time.Time, len(meta.Snapshots))
	for _, s := range meta.Snapshots {
		snapTimes[s.SnapshotID] = s.Timestamp()
	}
	// Fallback: if entry has no snapshot_id, use the current snapshot time.
	defaultTime := snap.Timestamp()

	const manifestReadWorkers = 4

	var (
		mu      sync.Mutex
		results []DataFileInfo
		sem     = make(chan struct{}, manifestReadWorkers)
	)

	g, gctx := errgroup.WithContext(ctx)

	for _, mf := range manifests {
		mf := mf
		g.Go(func() error {
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-gctx.Done():
				return gctx.Err()
			}

			entries, err := iceberg.ReadManifest(gctx, e.storage, mf.Path)
			if err != nil {
				log.Error().Err(err).Str("manifest", mf.Path).Msg("read manifest failed")
				return err
			}

			local := make([]DataFileInfo, 0, len(entries))
			for _, entry := range entries {
				if entry.DataFile.Content != 0 {
					continue // skip delete files
				}
				addedAt := defaultTime
				if entry.SnapshotID != nil {
					if t, ok := snapTimes[*entry.SnapshotID]; ok {
						addedAt = t
					}
				}
				local = append(local, DataFileInfo{
					Path:      entry.DataFile.FilePath,
					SizeBytes: entry.DataFile.FileSizeBytes,
					AddedAt:   addedAt,
				})
			}

			mu.Lock()
			results = append(results, local...)
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}

// selectCandidates groups files by partition and applies policy filters,
// returning one Candidate per eligible partition group.
func selectCandidates(id catalog.TableIdentifier, files []DataFileInfo, policy config.CompactionPolicy) []Candidate {
	if !policy.IsEnabled() || len(files) == 0 {
		return nil
	}

	minAge := time.Duration(policy.MinFileAgeMinutes) * time.Minute
	cutoff := time.Now().UTC().Add(-minAge)

	// Group files by partition (directory of the file path).
	groups := make(map[string][]DataFileInfo)
	for _, f := range files {
		partition := path.Dir(f.Path)
		if f.AddedAt.Before(cutoff) {
			groups[partition] = append(groups[partition], f)
		}
	}

	var candidates []Candidate
	for partition, group := range groups {
		if len(group) < policy.MinFileCount {
			continue
		}
		candidates = append(candidates, Candidate{
			Table:     id,
			Partition: partition,
			Files:     group,
			Policy:    policy,
		})
	}
	return candidates
}


