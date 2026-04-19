package compaction

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/basekick-labs/firn/internal/catalog"
	"github.com/basekick-labs/firn/internal/iceberg"
	"github.com/basekick-labs/firn/internal/storage"
	"github.com/rs/zerolog/log"
)

const (
	recoveryStatePending            = "pending"
	recoveryStateUploaded           = "uploaded"
	recoveryStateSnapshotCommitted  = "snapshot_committed"
)

// RecoveryManifest is written to storage before any file upload and deleted
// after source files are removed. It allows the daemon to resume interrupted
// jobs after a crash.
type RecoveryManifest struct {
	JobID          string   `json:"job_id"`
	Table          string   `json:"table"`        // "namespace.name"
	InputFiles     []string `json:"input_files"`
	OutputFile     string   `json:"output_file"`
	ManifestPath   string   `json:"manifest_path"`
	ManifestListPath string `json:"manifest_list_path"`
	ParentSnapshotID int64  `json:"parent_snapshot_id"`
	NewSnapshotID    int64  `json:"new_snapshot_id"`
	State          string   `json:"state"` // pending | uploaded | snapshot_committed
	CreatedAt      string   `json:"created_at"` // RFC3339
}

func writeRecovery(ctx context.Context, stor storage.Backend, path string, m RecoveryManifest) error {
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal recovery manifest: %w", err)
	}
	if err := stor.Write(ctx, path, bytes.NewReader(data), int64(len(data))); err != nil {
		return fmt.Errorf("write recovery manifest %s: %w", path, err)
	}
	return nil
}

func readRecovery(ctx context.Context, stor storage.Backend, path string) (RecoveryManifest, error) {
	rc, err := stor.Read(ctx, path)
	if err != nil {
		return RecoveryManifest{}, fmt.Errorf("read recovery manifest %s: %w", path, err)
	}
	defer rc.Close()
	var m RecoveryManifest
	if err := json.NewDecoder(rc).Decode(&m); err != nil {
		return RecoveryManifest{}, fmt.Errorf("decode recovery manifest %s: %w", path, err)
	}
	return m, nil
}

func deleteRecovery(ctx context.Context, stor storage.Backend, path string) error {
	if err := stor.Delete(ctx, path); err != nil {
		return fmt.Errorf("delete recovery manifest %s: %w", path, err)
	}
	return nil
}

// Recover scans the recovery directory under tableLocation and resumes any
// interrupted compaction jobs. It is safe to call on every daemon startup.
func Recover(ctx context.Context, stor storage.Backend, cat catalog.Client, tableID catalog.TableIdentifier, tableLocation string) error {
	prefix := tableLocation + "/.firn/recovery/"
	keys, err := stor.List(ctx, prefix)
	if err != nil {
		return fmt.Errorf("list recovery manifests: %w", err)
	}

	for _, key := range keys {
		if !strings.HasSuffix(key, ".json") {
			continue
		}
		if err := recoverOne(ctx, stor, cat, tableID, key); err != nil {
			log.Error().Err(err).Str("recovery_file", key).Msg("recovery failed for job")
		}
	}
	return nil
}

func recoverOne(ctx context.Context, stor storage.Backend, cat catalog.Client, tableID catalog.TableIdentifier, recoveryPath string) error {
	m, err := readRecovery(ctx, stor, recoveryPath)
	if err != nil {
		return err
	}

	log.Warn().Str("job_id", m.JobID).Str("state", m.State).Msg("recovering interrupted compaction job")

	switch m.State {
	case recoveryStatePending:
		// Output may not exist — just clean up the recovery manifest.
		log.Warn().Str("job_id", m.JobID).Msg("job was pending; discarding (output may be missing)")
		return deleteRecovery(ctx, stor, recoveryPath)

	case recoveryStateUploaded:
		// Output was uploaded but snapshot not committed — re-commit then clean up.
		if err := commitSnapshot(ctx, cat, stor, tableID, m); err != nil {
			return fmt.Errorf("re-commit snapshot for job %s: %w", m.JobID, err)
		}
		m.State = recoveryStateSnapshotCommitted
		if err := writeRecovery(ctx, stor, recoveryPath, m); err != nil {
			return err
		}
		fallthrough

	case recoveryStateSnapshotCommitted:
		// Snapshot committed — delete input files then the recovery manifest.
		deleteInputFiles(ctx, stor, m.InputFiles)
		return deleteRecovery(ctx, stor, recoveryPath)

	default:
		log.Error().Str("job_id", m.JobID).Str("state", m.State).Msg("unknown recovery state; skipping")
		return nil
	}
}

func commitSnapshot(ctx context.Context, cat catalog.Client, stor storage.Backend, tableID catalog.TableIdentifier, m RecoveryManifest) error {
	// Reload table to get the current snapshot ID for the requirement.
	meta, err := cat.LoadTable(ctx, tableID)
	if err != nil {
		return fmt.Errorf("load table: %w", err)
	}
	currentSnap := meta.CurrentSnapshot()
	var currentID int64
	if currentSnap != nil {
		currentID = currentSnap.SnapshotID
	}

	// Re-read the manifest entries to reconstruct the ManifestFile for the list.
	entries, err := iceberg.ReadManifest(ctx, stor, m.ManifestPath)
	if err != nil {
		return fmt.Errorf("read manifest for recovery: %w", err)
	}

	manifestFile := buildManifestFile(m.ManifestPath, m.NewSnapshotID, entries)

	manifestListKey, err := iceberg.URIToPath(m.ManifestListPath)
	if err != nil {
		return fmt.Errorf("resolve manifest list path: %w", err)
	}
	if err := iceberg.WriteManifestList(ctx, stor, manifestListKey, []iceberg.ManifestFile{manifestFile}); err != nil {
		return fmt.Errorf("rewrite manifest list for recovery: %w", err)
	}

	// Snapshot.ManifestList must be the full URI per the Iceberg spec.
	snap := buildSnapshot(m.NewSnapshotID, currentID, m.ManifestListPath)

	for attempt := 0; attempt < 3; attempt++ {
		tx := catalog.Transaction{
			Requirements: []catalog.Requirement{{Type: "assert-current-snapshot-id", CurrentSnapshotID: currentID}},
			Updates: []catalog.Update{
				{Type: "add-snapshot", Snapshot: &snap},
				{Type: "set-snapshot-ref", RefName: "main", SnapshotIDs: []int64{snap.SnapshotID}},
			},
		}
		err = cat.CommitTransaction(ctx, tableID, tx)
		if err == nil {
			return nil
		}
		var conflict catalog.ErrConflict
		if !errors.As(err, &conflict) {
			return err
		}
		// Reload and retry with fresh snapshot ID.
		meta, err = cat.LoadTable(ctx, tableID)
		if err != nil {
			return err
		}
		currentSnap = meta.CurrentSnapshot()
		if currentSnap != nil {
			currentID = currentSnap.SnapshotID
		}
	}
	return fmt.Errorf("commit failed after 3 attempts: %w", err)
}

func deleteInputFiles(ctx context.Context, stor storage.Backend, paths []string) {
	for _, p := range paths {
		key, err := iceberg.URIToPath(p)
		if err != nil {
			log.Error().Err(err).Str("path", p).Msg("failed to parse input file path; skipping delete")
			continue
		}
		if err := stor.Delete(ctx, key); err != nil {
			log.Error().Err(err).Str("path", p).Msg("failed to delete input file during recovery")
		}
	}
}

func buildManifestFile(manifestPath string, snapshotID int64, entries []iceberg.ManifestEntry) iceberg.ManifestFile {
	var added, existing int32
	var addedRows, existingRows int64
	for _, e := range entries {
		switch e.Status {
		case iceberg.EntryStatusAdded:
			added++
			addedRows += e.DataFile.RecordCount
		case iceberg.EntryStatusExisting:
			existing++
			existingRows += e.DataFile.RecordCount
		}
	}
	return iceberg.ManifestFile{
		Path:               manifestPath,
		Content:            iceberg.ManifestContentData,
		AddedSnapshotID:    snapshotID,
		AddedFilesCount:    added,
		ExistingFilesCount: existing,
		AddedRowsCount:     addedRows,
		ExistingRowsCount:  existingRows,
	}
}

func buildSnapshot(newSnapshotID, parentSnapshotID int64, manifestListPath string) iceberg.Snapshot {
	return iceberg.Snapshot{
		SnapshotID:       newSnapshotID,
		ParentSnapshotID: parentSnapshotID,
		TimestampMs:      time.Now().UnixMilli(),
		ManifestList:     manifestListPath,
		Summary:          map[string]string{"operation": "replace"},
	}
}
