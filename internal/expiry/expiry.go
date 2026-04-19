package expiry

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/basekick-labs/firn/internal/catalog"
	"github.com/basekick-labs/firn/internal/config"
	"github.com/basekick-labs/firn/internal/iceberg"
	"github.com/basekick-labs/firn/internal/storage"
	"github.com/rs/zerolog/log"
)

// Engine drives snapshot expiry for a single table.
type Engine struct {
	catalog catalog.Client
	storage storage.Backend
}

// NewEngine creates a new expiry Engine.
func NewEngine(cat catalog.Client, stor storage.Backend) *Engine {
	return &Engine{catalog: cat, storage: stor}
}

// Result is the outcome of one expiry run against a table.
type Result struct {
	Table            catalog.TableIdentifier
	ExpiredSnapshots int
	DeletedManifests int
	DeletedDataFiles int
	Duration         time.Duration
}

// ExecuteExpiry runs a full snapshot expiry cycle for the given table.
// It commits the removal atomically, then deletes manifest and data files
// that are no longer referenced by any live snapshot.
func (e *Engine) ExecuteExpiry(ctx context.Context, id catalog.TableIdentifier, policy config.SnapshotExpiry) (Result, error) {
	start := time.Now()
	now := start

	expiredIDs, preCommitMeta, err := e.commitWithRetry(ctx, id, policy, now)
	if err != nil {
		return Result{}, err
	}
	if len(expiredIDs) == 0 {
		log.Debug().Str("table", id.String()).Msg("no snapshots eligible for expiry")
		return Result{Table: id, Duration: time.Since(start)}, nil
	}

	expiredSet := make(map[int64]bool, len(expiredIDs))
	for _, sid := range expiredIDs {
		expiredSet[sid] = true
	}

	// Reload post-commit metadata so that any concurrent snapshot appended between
	// our LoadTable and the accepted commit is visible. collectManifestGarbage uses
	// this to build the live-manifest set, preventing deletion of manifests that are
	// already shared with a newly committed snapshot.
	postCommitMeta, err := e.catalog.LoadTable(ctx, id)
	if err != nil {
		// Non-fatal: fall back to preCommitMeta for GC. Conservative because we might
		// over-protect some files, but we will never delete live-referenced ones using
		// preCommitMeta (the committed snapshot list may be a superset of expiredSet,
		// so it's still safe to use expiredSet as the filter).
		log.Warn().Err(err).Str("table", id.String()).Msg("post-commit reload failed; using pre-commit metadata for GC")
		postCommitMeta = preCommitMeta
	}

	manifestLists, manifests, dataFiles, err := e.collectManifestGarbage(ctx, postCommitMeta, expiredSet)
	if err != nil {
		// Garbage collection failure is non-fatal — snapshot metadata is already cleaned up.
		// Orphan cleanup will handle the residual files.
		log.Warn().Err(err).Str("table", id.String()).Msg("manifest garbage collection failed; orphan cleanup will recover")
	} else {
		// Delete in order: manifest lists → manifests → data files.
		deleteFiles(ctx, e.storage, manifestLists)
		deleteFiles(ctx, e.storage, manifests)
		deleteFiles(ctx, e.storage, dataFiles)
	}

	return Result{
		Table:            id,
		ExpiredSnapshots: len(expiredIDs),
		DeletedManifests: len(manifestLists) + len(manifests),
		DeletedDataFiles: len(dataFiles),
		Duration:         time.Since(start),
	}, nil
}

// SelectExpired returns the IDs of snapshots eligible for deletion given the
// table metadata, policy, and reference time. It is a pure function with no
// I/O side effects.
//
// A snapshot is eligible when ALL of the following hold:
//   - It is not the current snapshot or any of its ancestors.
//   - Its age exceeds MaxSnapshotAgeHours.
//
// The result is further clamped so that at least MinSnapshotsToKeep snapshots
// remain in the table after deletion.
func SelectExpired(meta *iceberg.TableMetadata, policy config.SnapshotExpiry, now time.Time) []int64 {
	if !policy.Enabled || len(meta.Snapshots) == 0 {
		return nil
	}

	// Build the ancestry set by walking the parent chain from the current snapshot.
	ancestorIDs := make(map[int64]bool)
	cursor := meta.CurrentSnapshotID
	for cursor != 0 {
		snap := meta.SnapshotByID(cursor)
		if snap == nil {
			break
		}
		ancestorIDs[cursor] = true
		cursor = snap.ParentSnapshotID
	}

	cutoff := now.Add(-time.Duration(policy.MaxSnapshotAgeHours) * time.Hour)

	// Sort oldest-first so that when the MinSnapshotsToKeep floor applies, we
	// retain the oldest expirables and delete only the newest ones.
	sorted := make([]iceberg.Snapshot, len(meta.Snapshots))
	copy(sorted, meta.Snapshots)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].TimestampMs < sorted[j].TimestampMs
	})

	var expirable []iceberg.Snapshot
	for _, s := range sorted {
		if !ancestorIDs[s.SnapshotID] && s.Timestamp().Before(cutoff) {
			expirable = append(expirable, s)
		}
	}

	if len(expirable) == 0 {
		return nil
	}

	// Apply the minimum-count floor: mustRetain is the number of expirable snapshots
	// that must be kept to satisfy MinSnapshotsToKeep. We keep the oldest expirables
	// (front of the slice) and delete only the newer ones (tail).
	kept := len(meta.Snapshots) - len(expirable)
	if kept < policy.MinSnapshotsToKeep {
		mustRetain := policy.MinSnapshotsToKeep - kept
		if mustRetain >= len(expirable) {
			return nil
		}
		expirable = expirable[mustRetain:]
	}

	ids := make([]int64, len(expirable))
	for i, s := range expirable {
		ids[i] = s.SnapshotID
	}
	return ids
}

// commitWithRetry commits the remove-snapshots transaction, retrying up to 3 times on conflict.
// It reloads the table and re-evaluates SelectExpired on each attempt so that the expired set
// always reflects the current ancestry chain.
//
// Returns the expired IDs and the pre-commit metadata. Returns (nil, meta, nil) when nothing
// is eligible for expiry.
func (e *Engine) commitWithRetry(
	ctx context.Context,
	id catalog.TableIdentifier,
	policy config.SnapshotExpiry,
	now time.Time,
) ([]int64, *iceberg.TableMetadata, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		meta, err := e.catalog.LoadTable(ctx, id)
		if err != nil {
			return nil, nil, fmt.Errorf("load table: %w", err)
		}

		expiredIDs := SelectExpired(meta, policy, now)
		if len(expiredIDs) == 0 {
			return nil, meta, nil
		}

		tx := catalog.Transaction{
			Requirements: []catalog.Requirement{
				{Type: "assert-current-snapshot-id", CurrentSnapshotID: meta.CurrentSnapshotID},
			},
			Updates: []catalog.Update{
				{Type: "remove-snapshots", SnapshotIDs: expiredIDs},
			},
		}

		lastErr = e.catalog.CommitTransaction(ctx, id, tx)
		if lastErr == nil {
			return expiredIDs, meta, nil
		}

		var conflict catalog.ErrConflict
		if !errors.As(lastErr, &conflict) {
			return nil, nil, lastErr
		}
		log.Debug().Str("table", id.String()).Int("attempt", attempt+1).Msg("snapshot expiry commit conflict, retrying")
	}
	return nil, nil, fmt.Errorf("commit failed after 3 attempts: %w", lastErr)
}

// collectManifestGarbage identifies manifest lists, manifests, and data files that are
// exclusively referenced by expired snapshots and can be safely deleted.
//
// meta must be the post-commit table metadata so that any concurrently added snapshot's
// manifest references are included in the live set.
//
// The algorithm is conservative: a file is only included in the deletion set if it
// does not appear in any live (non-expired) snapshot's manifest chain.
func (e *Engine) collectManifestGarbage(
	ctx context.Context,
	meta *iceberg.TableMetadata,
	expiredSet map[int64]bool,
) (manifestLists, manifests, dataFiles []string, err error) {
	// Pass 1: collect manifest paths referenced by live snapshots.
	liveManifests := make(map[string]bool)
	for i := range meta.Snapshots {
		s := &meta.Snapshots[i]
		if expiredSet[s.SnapshotID] || s.ManifestList == "" {
			continue
		}
		mfs, readErr := iceberg.ReadManifestList(ctx, e.storage, s.ManifestList)
		if readErr != nil {
			return nil, nil, nil, fmt.Errorf("read live manifest list %s: %w", s.ManifestList, readErr)
		}
		for _, mf := range mfs {
			liveManifests[mf.Path] = true
		}
	}

	// Pass 2: collect expired manifest lists and identify exclusive manifests.
	exclusiveManifests := make(map[string]bool)
	for i := range meta.Snapshots {
		s := &meta.Snapshots[i]
		if !expiredSet[s.SnapshotID] || s.ManifestList == "" {
			continue
		}
		p, uriErr := iceberg.URIToPath(s.ManifestList)
		if uriErr != nil {
			log.Warn().Err(uriErr).Str("uri", s.ManifestList).Msg("cannot convert manifest list URI, skipping")
		} else {
			manifestLists = append(manifestLists, p)
		}
		mfs, readErr := iceberg.ReadManifestList(ctx, e.storage, s.ManifestList)
		if readErr != nil {
			log.Warn().Err(readErr).Str("uri", s.ManifestList).Msg("cannot read expired manifest list, skipping")
			continue
		}
		for _, mf := range mfs {
			if !liveManifests[mf.Path] {
				exclusiveManifests[mf.Path] = true
			}
		}
	}

	// Pass 3: collect data file candidates from exclusive manifests.
	dataFileCandidates := make(map[string]bool)
	for manifestURI := range exclusiveManifests {
		entries, readErr := iceberg.ReadManifest(ctx, e.storage, manifestURI)
		if readErr != nil {
			log.Warn().Err(readErr).Str("uri", manifestURI).Msg("cannot read expired manifest, skipping data file collection")
			continue
		}
		for _, entry := range entries {
			dataFileCandidates[entry.DataFile.FilePath] = true
		}
	}

	// Pass 4: collect live data files so we can subtract them.
	liveDataFiles := make(map[string]bool)
	for manifestURI := range liveManifests {
		entries, readErr := iceberg.ReadManifest(ctx, e.storage, manifestURI)
		if readErr != nil {
			// Conservative: abort data-file GC on error rather than risk deleting live files.
			log.Warn().Err(readErr).Str("uri", manifestURI).Msg("cannot read live manifest, skipping data file GC")
			return manifestLists, nil, nil, nil
		}
		for _, entry := range entries {
			liveDataFiles[entry.DataFile.FilePath] = true
		}
	}

	// Convert exclusive manifest URIs to storage keys.
	for uri := range exclusiveManifests {
		p, uriErr := iceberg.URIToPath(uri)
		if uriErr != nil {
			log.Warn().Err(uriErr).Str("uri", uri).Msg("cannot convert manifest URI, skipping")
			continue
		}
		manifests = append(manifests, p)
	}

	// Data files safe to delete: candidates not referenced by any live manifest.
	for uri := range dataFileCandidates {
		if liveDataFiles[uri] {
			continue
		}
		p, uriErr := iceberg.URIToPath(uri)
		if uriErr != nil {
			log.Warn().Err(uriErr).Str("uri", uri).Msg("cannot convert data file URI, skipping")
			continue
		}
		dataFiles = append(dataFiles, p)
	}

	return manifestLists, manifests, dataFiles, nil
}

// deleteFiles deletes each path from storage, logging a warning on failure.
func deleteFiles(ctx context.Context, stor storage.Backend, paths []string) {
	for _, p := range paths {
		if err := stor.Delete(ctx, p); err != nil {
			log.Warn().Err(err).Str("path", p).Msg("failed to delete file during snapshot expiry")
		}
	}
}
