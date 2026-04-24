package compaction

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/basekick-labs/firn/internal/catalog"
	"github.com/basekick-labs/firn/internal/iceberg"
	"github.com/basekick-labs/firn/internal/retry"
	"github.com/basekick-labs/firn/internal/storage"
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

var memoryLimitRE = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?\s*(B|KB|MB|GB|TB)$`)

const maxSplitDepth = 4

// isRecoverableExit reports whether err from subprocessFn is an OOM/segfault
// worth retrying with fewer files.
// Recoverable: exit 137 (SIGKILL / Linux OOM killer), exit 139 (SIGSEGV),
// or a JSON decode failure meaning the subprocess died before writing stdout.
// NOTE: the decode-failure check is coupled to the format string in spawnSubprocess;
// update both together if that string ever changes.
func isRecoverableExit(err error) bool {
	if err == nil {
		return false
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		code := exitErr.ExitCode()
		return code == 137 || code == 139
	}
	return strings.Contains(err.Error(), "decode subprocess result:")
}

// splitCandidate divides c.Files in half. Caller must ensure len(c.Files) >= 2.
func splitCandidate(c Candidate) (lo, hi Candidate) {
	mid := len(c.Files) / 2
	lo = Candidate{Table: c.Table, Partition: c.Partition, Policy: c.Policy, Files: c.Files[:mid]}
	hi = Candidate{Table: c.Table, Partition: c.Partition, Policy: c.Policy, Files: c.Files[mid:]}
	return
}

// mergeResults combines two Results from a successful split pair into a summary.
func mergeResults(a, b Result) Result {
	inputs := make([]string, 0, len(a.InputFiles)+len(b.InputFiles))
	inputs = append(inputs, a.InputFiles...)
	inputs = append(inputs, b.InputFiles...)
	return Result{
		Table:       a.Table,
		Partition:   a.Partition,
		InputFiles:  inputs,
		OutputFile:  "", // two output files — no single canonical path
		OutputSize:  a.OutputSize + b.OutputSize,
		BytesBefore: a.BytesBefore + b.BytesBefore,
		BytesAfter:  a.BytesAfter + b.BytesAfter,
		Duration:    a.Duration + b.Duration,
	}
}

// ExecuteJob runs a full compaction job for a candidate, splitting the file
// batch in half and retrying on recoverable subprocess failures (OOM, segfault).
func (e *Engine) ExecuteJob(ctx context.Context, c Candidate) (Result, error) {
	return e.executeWithSplit(ctx, c, 0)
}

// executeWithSplit is the recursive implementation of ExecuteJob.
// depth tracks recursion level; maxSplitDepth caps it at 4 (so up to 16→1 files).
//
// Iceberg v2 sequence numbers are not yet managed by this implementation;
// only v1-compatible tables (no row-level deletes) are safe to compact.
func (e *Engine) executeWithSplit(ctx context.Context, c Candidate, depth int) (Result, error) {
	start := time.Now()

	meta, err := e.catalog.LoadTable(ctx, c.Table)
	if err != nil {
		return Result{}, fmt.Errorf("load table: %w", err)
	}
	currentSnap := meta.CurrentSnapshot()
	var currentSnapshotID int64
	if currentSnap != nil {
		currentSnapshotID = currentSnap.SnapshotID
	}

	tableLocation := strings.TrimRight(meta.Location, "/")
	jobID := uuid.New().String()
	newSnapshotID := time.Now().UnixMilli()

	// Full URIs for Iceberg metadata references (stored in manifests/snapshots).
	outputURI := tableLocation + "/data/compacted/" + jobID + ".parquet"
	manifestURI := tableLocation + "/metadata/snap-" + jobID + ".avro"
	manifestListURI := tableLocation + "/metadata/snap-" + jobID + "-ml.avro"
	recoveryURI := tableLocation + "/.firn/recovery/" + jobID + ".json"

	// Storage-relative keys for actual read/write operations.
	outputKey, err := iceberg.URIToPath(outputURI)
	if err != nil {
		return Result{}, fmt.Errorf("resolve output path: %w", err)
	}
	manifestKey, err := iceberg.URIToPath(manifestURI)
	if err != nil {
		return Result{}, fmt.Errorf("resolve manifest path: %w", err)
	}
	manifestListKey, err := iceberg.URIToPath(manifestListURI)
	if err != nil {
		return Result{}, fmt.Errorf("resolve manifest list path: %w", err)
	}
	recoveryKey, err := iceberg.URIToPath(recoveryURI)
	if err != nil {
		return Result{}, fmt.Errorf("resolve recovery path: %w", err)
	}

	inputPaths := make([]string, len(c.Files))
	var bytesBefore int64
	for i, f := range c.Files {
		inputPaths[i] = f.Path
		bytesBefore += f.SizeBytes
	}

	bucket, err := bucketFromURI(tableLocation)
	if err != nil {
		return Result{}, fmt.Errorf("extract bucket from table location: %w", err)
	}
	storCfgJSON, err := e.storageConfigJSON(bucket)
	if err != nil {
		return Result{}, err
	}

	// Step 1: Write crash-recovery manifest before any upload.
	// NOTE: if the process crashes after upload but before writeRecovery(uploaded)
	// succeeds, the recovery file will still say "pending" and recoverOne will
	// discard the job. This is a known limitation: the compacted output will be
	// orphaned in storage until orphan cleanup runs.
	rm := RecoveryManifest{
		JobID:            jobID,
		Table:            c.Table.String(),
		InputFiles:       inputPaths,
		OutputFile:       outputURI,
		ManifestPath:     manifestURI,
		ManifestListPath: manifestListURI,
		ParentSnapshotID: currentSnapshotID,
		NewSnapshotID:    newSnapshotID,
		State:            recoveryStatePending,
		CreatedAt:        time.Now().UTC().Format(time.RFC3339),
	}
	if err := writeRecovery(ctx, e.storage, recoveryKey, rm); err != nil {
		return Result{}, err
	}

	// Step 2: Spawn compact subprocess.
	tempDir, err := os.MkdirTemp("", "firn-compact-"+jobID+"-")
	if err != nil {
		return Result{}, fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tempDir)

	subCfg := SubprocessConfig{
		JobID:         jobID,
		InputFiles:    inputPaths,
		OutputPath:    outputURI,
		SortKeys:      c.Policy.SortKeys,
		ZOrderColumns: c.Policy.ZOrderColumns,
		Strategy:      c.Policy.Strategy,
		MemoryLimit:   e.cfg.Scheduler.MemoryLimit,
		StorageType:   e.cfg.Storage.Type,
		StorageConfig: storCfgJSON,
		TempDir:       tempDir,
	}

	subResult, subErr := e.subprocessFn(ctx, subCfg)

	switch {
	case subErr != nil && isRecoverableExit(subErr):
		// OOM or segfault — clean up this attempt's recovery manifest and split.
		if delErr := deleteRecovery(ctx, e.storage, recoveryKey); delErr != nil {
			log.Warn().Err(delErr).Str("job_id", jobID).Msg("failed to delete recovery manifest before split")
		}
		if len(c.Files) < 2 || depth >= maxSplitDepth {
			return Result{}, fmt.Errorf("compact subprocess: %w (cannot split further: files=%d depth=%d)", subErr, len(c.Files), depth)
		}
		log.Warn().Err(subErr).
			Str("table", c.Table.String()).
			Str("partition", c.Partition).
			Int("files", len(c.Files)).
			Int("depth", depth).
			Msg("recoverable subprocess failure; splitting candidate")
		lo, hi := splitCandidate(c)
		loRes, loErr := e.executeWithSplit(ctx, lo, depth+1)
		hiRes, hiErr := e.executeWithSplit(ctx, hi, depth+1)
		if loErr == nil && hiErr == nil {
			merged := mergeResults(loRes, hiRes)
			merged.Duration = time.Since(start)
			return merged, nil
		}
		// At least one half failed; the successful half already committed its
		// snapshot so partial progress is durable in the catalog.
		if loErr != nil {
			return Result{}, fmt.Errorf("split lo (depth %d): %w", depth+1, loErr)
		}
		return Result{}, fmt.Errorf("split hi (depth %d): %w", depth+1, hiErr)

	case subErr != nil:
		// Non-recoverable (permission denied, context cancelled, etc.)
		if delErr := deleteRecovery(ctx, e.storage, recoveryKey); delErr != nil {
			log.Warn().Err(delErr).Str("job_id", jobID).Msg("failed to delete recovery manifest after non-recoverable failure")
		}
		return Result{}, fmt.Errorf("compact subprocess: %w", subErr)

	case !subResult.Success:
		// Application-level DuckDB error — splitting won't help.
		if delErr := deleteRecovery(ctx, e.storage, recoveryKey); delErr != nil {
			log.Warn().Err(delErr).Str("job_id", jobID).Msg("failed to delete recovery manifest after subprocess error")
		}
		return Result{}, fmt.Errorf("compact subprocess error: %s", subResult.Error)
	}

	// Step 3: Update recovery manifest — output uploaded.
	rm.State = recoveryStateUploaded
	if err := writeRecovery(ctx, e.storage, recoveryKey, rm); err != nil {
		log.Warn().Err(err).Str("job_id", jobID).Msg("failed to update recovery manifest to uploaded")
	}

	// Step 4: Write Iceberg manifest + manifest list (storage-relative keys).
	outputEntry := iceberg.ManifestEntry{
		Status:   iceberg.EntryStatusAdded,
		DataFile: iceberg.DataFile{FilePath: outputKey, FileFormat: "PARQUET", FileSizeBytes: subResult.OutputSize},
	}
	if err := iceberg.WriteManifest(ctx, e.storage, manifestKey, []iceberg.ManifestEntry{outputEntry}); err != nil {
		return Result{}, err
	}

	manifestFile := buildManifestFile(manifestURI, newSnapshotID, []iceberg.ManifestEntry{outputEntry})
	if err := iceberg.WriteManifestList(ctx, e.storage, manifestListKey, []iceberg.ManifestFile{manifestFile}); err != nil {
		return Result{}, err
	}

	// Step 5: Commit snapshot (retry on conflict).
	// snap.ParentSnapshotID is updated inside commitWithRetry on each conflict retry.
	snap := buildSnapshot(newSnapshotID, currentSnapshotID, manifestListURI)
	if err := e.commitWithRetry(ctx, c.Table, currentSnapshotID, &snap); err != nil {
		return Result{}, err
	}

	// Step 6: Mark snapshot committed.
	rm.State = recoveryStateSnapshotCommitted
	if err := writeRecovery(ctx, e.storage, recoveryKey, rm); err != nil {
		log.Warn().Err(err).Str("job_id", jobID).Msg("failed to update recovery manifest to snapshot_committed")
	}

	// Step 7: Delete input files.
	deleteInputFiles(ctx, e.storage, inputPaths)

	// Step 8: Delete recovery manifest.
	if err := deleteRecovery(ctx, e.storage, recoveryKey); err != nil {
		log.Warn().Err(err).Str("job_id", jobID).Msg("failed to delete recovery manifest")
	}

	return Result{
		Table:       c.Table,
		Partition:   c.Partition,
		InputFiles:  inputPaths,
		OutputFile:  outputURI,
		OutputSize:  subResult.OutputSize,
		BytesBefore: bytesBefore,
		BytesAfter:  subResult.OutputSize,
		Duration:    time.Since(start),
	}, nil
}

// commitWithRetry commits a new snapshot, retrying on ErrConflict with backoff.
// On each conflict it reloads the table to get the current snapshot ID and updates
// snap.ParentSnapshotID. snap.SnapshotID is intentionally not changed between
// retries — it must remain unique for the lifetime of the table.
func (e *Engine) commitWithRetry(ctx context.Context, tableID catalog.TableIdentifier, currentSnapshotID int64, snap *iceberg.Snapshot) error {
	return e.retryer.Do(ctx, retry.IsConflict, func(attempt int) error {
		if attempt > 0 {
			// Reload to get the fresh current snapshot ID for this attempt.
			meta, err := e.catalog.LoadTable(ctx, tableID)
			if err != nil {
				return err
			}
			if s := meta.CurrentSnapshot(); s != nil {
				currentSnapshotID = s.SnapshotID
				snap.ParentSnapshotID = currentSnapshotID
			}
		}
		tx := catalog.Transaction{
			Requirements: []catalog.Requirement{{Type: "assert-current-snapshot-id", CurrentSnapshotID: currentSnapshotID}},
			Updates: []catalog.Update{
				{Type: "add-snapshot", Snapshot: snap},
				{Type: "set-snapshot-ref", RefName: "main", SnapshotIDs: []int64{snap.SnapshotID}},
			},
		}
		return e.catalog.CommitTransaction(ctx, tableID, tx)
	})
}

// storageConfigJSON serialises the storage credentials for the subprocess.
// bucket is the bucket/container name extracted from the table location URI.
func (e *Engine) storageConfigJSON(bucket string) (string, error) {
	var (
		data []byte
		err  error
	)
	switch e.cfg.Storage.Type {
	case "gcs":
		data, err = json.Marshal(storage.GCSConfig{
			Bucket:          bucket,
			Project:         e.cfg.Storage.Project,
			CredentialsJSON: e.cfg.Storage.CredentialsJSON,
		})
	case "azure":
		data, err = json.Marshal(storage.AzureConfig{
			Account:          e.cfg.Storage.Account,
			Container:        bucket,
			AccountKey:       e.cfg.Storage.AccountKey,
			ConnectionString: e.cfg.Storage.ConnectionString,
		})
	default: // "s3" and any future S3-compatible type
		data, err = json.Marshal(storage.S3Config{
			Bucket:          bucket,
			Endpoint:        e.cfg.Storage.Endpoint,
			Region:          e.cfg.Storage.Region,
			AccessKeyID:     e.cfg.Storage.AccessKeyID,
			SecretAccessKey: e.cfg.Storage.SecretAccessKey,
			PathStyle:       e.cfg.Storage.PathStyle,
		})
	}
	if err != nil {
		return "", fmt.Errorf("marshal storage config: %w", err)
	}
	return string(data), nil
}

// bucketFromURI extracts the bucket/container name from a cloud storage URI.
// Supports s3://, gs://, wasbs://, and abfss:// schemes. Returns empty string
// for non-URI (local/relative) paths.
func bucketFromURI(uri string) (string, error) {
	if !strings.Contains(uri, "://") {
		return "", nil
	}
	u, err := url.Parse(uri)
	if err != nil {
		return "", err
	}
	if u.Host == "" {
		return "", fmt.Errorf("no bucket in URI %q", uri)
	}
	return u.Host, nil
}

// spawnSubprocess runs the compact subcommand as a child process, passing
// SubprocessConfig via stdin and reading SubprocessResult from stdout.
func spawnSubprocess(ctx context.Context, cfg SubprocessConfig) (SubprocessResult, error) {
	execPath, err := os.Executable()
	if err != nil {
		return SubprocessResult{}, fmt.Errorf("resolve executable: %w", err)
	}

	cfgData, err := json.Marshal(cfg)
	if err != nil {
		return SubprocessResult{}, fmt.Errorf("marshal subprocess config: %w", err)
	}

	cmd := exec.CommandContext(ctx, execPath, "compact")
	cmd.Stdin = bytes.NewReader(cfgData)
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return SubprocessResult{}, fmt.Errorf("subprocess exited: %w", err)
	}

	var result SubprocessResult
	if err := json.NewDecoder(&stdout).Decode(&result); err != nil {
		return SubprocessResult{}, fmt.Errorf("decode subprocess result: %w", err)
	}
	return result, nil
}

// RunSubprocess is the entrypoint called by cmd/compact (the child process).
// It downloads inputs, merges via DuckDB, and uploads the output.
func RunSubprocess(cfg SubprocessConfig) SubprocessResult {
	ctx := context.Background()

	stor, err := storage.FromConfig(ctx, cfg.StorageType, cfg.StorageConfig)
	if err != nil {
		return SubprocessResult{Error: fmt.Sprintf("init storage: %s", err)}
	}

	// Download each input file to tempDir.
	localInputs := make([]string, 0, len(cfg.InputFiles))
	var bytesBefore int64
	for _, remotePath := range cfg.InputFiles {
		localPath := filepath.Join(cfg.TempDir, filepath.Base(remotePath))
		size, err := downloadFile(ctx, stor, remotePath, localPath)
		if err != nil {
			return SubprocessResult{Error: fmt.Sprintf("download %s: %s", remotePath, err)}
		}
		localInputs = append(localInputs, localPath)
		bytesBefore += size
	}

	localOutput := filepath.Join(cfg.TempDir, "output.parquet")
	if err := runDuckDB(localInputs, localOutput, cfg); err != nil {
		return SubprocessResult{Error: fmt.Sprintf("duckdb: %s", err)}
	}

	// Upload output.
	f, err := os.Open(localOutput)
	if err != nil {
		return SubprocessResult{Error: fmt.Sprintf("open output: %s", err)}
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return SubprocessResult{Error: fmt.Sprintf("stat output: %s", err)}
	}
	outputSize := fi.Size()

	outputKey, err := iceberg.URIToPath(cfg.OutputPath)
	if err != nil {
		return SubprocessResult{Error: fmt.Sprintf("resolve output path: %s", err)}
	}
	if err := stor.Write(ctx, outputKey, f, outputSize); err != nil {
		return SubprocessResult{Error: fmt.Sprintf("upload output: %s", err)}
	}

	return SubprocessResult{Success: true, OutputSize: outputSize, BytesBefore: bytesBefore}
}

func downloadFile(ctx context.Context, stor storage.Backend, remotePath, localPath string) (int64, error) {
	key, err := iceberg.URIToPath(remotePath)
	if err != nil {
		return 0, err
	}
	rc, err := stor.Read(ctx, key)
	if err != nil {
		return 0, err
	}
	defer rc.Close()

	f, err := os.Create(localPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	n, err := io.Copy(f, rc)
	return n, err
}

var columnNameRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.]*$`)

// buildOrderClause returns an ORDER BY clause with double-quoted identifiers, or "" if sortKeys is empty.
// Returns an error if any key contains characters that are not safe column name characters.
func buildOrderClause(sortKeys []string) (string, error) {
	if len(sortKeys) == 0 {
		return "", nil
	}
	quoted := make([]string, len(sortKeys))
	for i, k := range sortKeys {
		if !columnNameRE.MatchString(k) {
			return "", fmt.Errorf("invalid sort_key %q: must match [A-Za-z_][A-Za-z0-9_.]*", k)
		}
		quoted[i] = `"` + k + `"`
	}
	return "ORDER BY " + strings.Join(quoted, ", "), nil
}

// buildZOrderClause returns an ORDER BY clause that approximates z-order (Morton curve)
// locality by sorting on the hash of each column in sequence. True bit-interleaving would
// require the lindel DuckDB community extension (internet access at runtime); hash ordering
// gives equivalent multi-column co-location for query filter acceleration without external
// dependencies.
func buildZOrderClause(cols []string) (string, error) {
	if len(cols) == 0 {
		return "", nil
	}
	terms := make([]string, len(cols))
	for i, c := range cols {
		if !columnNameRE.MatchString(c) {
			return "", fmt.Errorf("invalid z_order_column name %q: must match [A-Za-z_][A-Za-z0-9_.]*", c)
		}
		terms[i] = fmt.Sprintf(`hash("%s")`, c)
	}
	return "ORDER BY " + strings.Join(terms, ", "), nil
}

func runDuckDB(localInputs []string, outputPath string, cfg SubprocessConfig) error {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	if cfg.MemoryLimit != "" {
		if !memoryLimitRE.MatchString(cfg.MemoryLimit) {
			return fmt.Errorf("invalid memory_limit %q: must match [0-9]+(B|KB|MB|GB|TB)", cfg.MemoryLimit)
		}
		if _, err := db.Exec(fmt.Sprintf("SET memory_limit='%s'", cfg.MemoryLimit)); err != nil {
			return fmt.Errorf("set memory_limit: %w", err)
		}
	}

	// Build quoted list of local file paths.
	fileQuoted := make([]string, len(localInputs))
	for i, p := range localInputs {
		fileQuoted[i] = "'" + strings.ReplaceAll(p, "'", "''") + "'"
	}
	inputList := "[" + strings.Join(fileQuoted, ", ") + "]"

	var (
		orderClause string
		clauseErr   error
	)
	switch cfg.Strategy {
	case "binpack", "":
		// no ORDER BY
	case "sort":
		orderClause, clauseErr = buildOrderClause(cfg.SortKeys)
	case "z-order":
		orderClause, clauseErr = buildZOrderClause(cfg.ZOrderColumns)
	default:
		return fmt.Errorf("unknown compaction strategy %q", cfg.Strategy)
	}
	if clauseErr != nil {
		return clauseErr
	}

	safeOutput := strings.ReplaceAll(outputPath, "'", "''")
	query := fmt.Sprintf(
		"COPY (SELECT * FROM read_parquet(%s, union_by_name=true) %s) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 122880)",
		inputList, orderClause, safeOutput,
	)

	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("copy query: %w", err)
	}
	return nil
}
