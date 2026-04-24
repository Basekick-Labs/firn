package compaction

import (
	"context"
	"fmt"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/firn/internal/catalog"
	"github.com/basekick-labs/firn/internal/config"
	"github.com/basekick-labs/firn/internal/retry"
	"github.com/basekick-labs/firn/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeExitError produces a real *exec.ExitError with the given exit code by
// running a shell command. Required because errors.As only matches *exec.ExitError.
func makeExitError(t *testing.T, code int) error {
	t.Helper()
	cmd := exec.Command("sh", "-c", fmt.Sprintf("exit %d", code))
	err := cmd.Run()
	require.Error(t, err, "expected non-zero exit")
	return fmt.Errorf("subprocess exited: %w", err)
}

// --- isRecoverableExit ---

func TestIsRecoverableExit(t *testing.T) {
	tests := []struct {
		name      string
		err       func(t *testing.T) error
		want      bool
	}{
		{
			name: "nil",
			err:  func(t *testing.T) error { return nil },
			want: false,
		},
		{
			name: "exit 137 (OOM/SIGKILL)",
			err:  func(t *testing.T) error { return makeExitError(t, 137) },
			want: true,
		},
		{
			name: "exit 139 (SIGSEGV)",
			err:  func(t *testing.T) error { return makeExitError(t, 139) },
			want: true,
		},
		{
			name: "exit 1 (generic failure)",
			err:  func(t *testing.T) error { return makeExitError(t, 1) },
			want: false,
		},
		{
			name: "decode subprocess result error",
			err:  func(t *testing.T) error { return fmt.Errorf("decode subprocess result: unexpected EOF") },
			want: true,
		},
		{
			name: "plain error",
			err:  func(t *testing.T) error { return fmt.Errorf("permission denied") },
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isRecoverableExit(tt.err(t)))
		})
	}
}

// --- splitCandidate ---

func TestSplitCandidate(t *testing.T) {
	makeFiles := func(n int) []DataFileInfo {
		files := make([]DataFileInfo, n)
		for i := range files {
			files[i] = DataFileInfo{Path: fmt.Sprintf("file%d.parquet", i), SizeBytes: 100}
		}
		return files
	}
	c := Candidate{
		Table:     tableID,
		Partition: "dt=2024",
		Policy:    config.CompactionPolicy{},
	}

	tests := []struct {
		name    string
		n       int
		wantLo  int
		wantHi  int
	}{
		{"even 4", 4, 2, 2},
		{"odd 5", 5, 2, 3},
		{"minimum 2", 2, 1, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c.Files = makeFiles(tt.n)
			lo, hi := splitCandidate(c)
			assert.Len(t, lo.Files, tt.wantLo)
			assert.Len(t, hi.Files, tt.wantHi)
			assert.Equal(t, tt.n, len(lo.Files)+len(hi.Files))
			// Verify Table/Partition/Policy are copied.
			assert.Equal(t, c.Table, lo.Table)
			assert.Equal(t, c.Partition, lo.Partition)
			assert.Equal(t, c.Table, hi.Table)
		})
	}
}

// --- mergeResults ---

func TestMergeResults(t *testing.T) {
	a := Result{
		Table: tableID, Partition: "p",
		InputFiles: []string{"a1.parquet", "a2.parquet"},
		OutputFile: "out-a.parquet", OutputSize: 500,
		BytesBefore: 1000, BytesAfter: 500, Duration: 2 * time.Second,
	}
	b := Result{
		Table: tableID, Partition: "p",
		InputFiles: []string{"b1.parquet"},
		OutputFile: "out-b.parquet", OutputSize: 300,
		BytesBefore: 600, BytesAfter: 300, Duration: 1 * time.Second,
	}
	got := mergeResults(a, b)

	assert.Equal(t, tableID, got.Table)
	assert.Equal(t, "p", got.Partition)
	assert.Equal(t, []string{"a1.parquet", "a2.parquet", "b1.parquet"}, got.InputFiles)
	assert.Empty(t, got.OutputFile, "merged result must not name a single output file")
	assert.Equal(t, int64(800), got.OutputSize)
	assert.Equal(t, int64(1600), got.BytesBefore)
	assert.Equal(t, int64(800), got.BytesAfter)
	assert.Equal(t, 3*time.Second, got.Duration)
}

// --- executeWithSplit integration tests ---

// newSplitEngine builds an Engine with mock catalog and in-memory storage,
// ready for split tests. The table metadata is pre-seeded with snapshotID 1
// at location "s3://bucket/ns/tbl".
func newSplitEngine(cat catalog.Client) (*Engine, *testutil.MemStorage) {
	stor := testutil.NewMemStorage(nil)
	cfg := &config.Config{
		Storage:   config.StorageConfig{Type: "s3"},
		Scheduler: config.SchedulerConfig{MemoryLimit: "1GB"},
	}
	e := NewEngine(cat, stor, cfg, retry.New(retry.Config{MaxAttempts: 3}))
	return e, stor
}

// makeCandidate builds a Candidate with n files at location "s3://bucket/ns/tbl/data/".
func makeCandidate(n int) Candidate {
	files := make([]DataFileInfo, n)
	for i := range files {
		files[i] = DataFileInfo{
			Path:      fmt.Sprintf("s3://bucket/ns/tbl/data/file%d.parquet", i),
			SizeBytes: 100,
		}
	}
	return Candidate{
		Table:     tableID,
		Partition: "s3://bucket/ns/tbl/data",
		Files:     files,
		Policy:    config.CompactionPolicy{Strategy: "binpack"},
	}
}

// successFn returns a subprocessFn that always succeeds.
func successFn() func(context.Context, SubprocessConfig) (SubprocessResult, error) {
	return func(_ context.Context, cfg SubprocessConfig) (SubprocessResult, error) {
		return SubprocessResult{Success: true, OutputSize: 200, BytesBefore: 400}, nil
	}
}

// oomOnFirstN returns a subprocessFn that returns exit-137 for the first n
// calls, then succeeds. Uses an atomic counter for safe concurrent access.
func oomOnFirstN(n int32) func(context.Context, SubprocessConfig) (SubprocessResult, error) {
	var calls atomic.Int32
	return func(_ context.Context, cfg SubprocessConfig) (SubprocessResult, error) {
		if calls.Add(1) <= n {
			return SubprocessResult{}, makeExitError137()
		}
		return SubprocessResult{Success: true, OutputSize: 200, BytesBefore: 400}, nil
	}
}

// alwaysOOM returns a subprocessFn that always returns exit-137.
func alwaysOOM() func(context.Context, SubprocessConfig) (SubprocessResult, error) {
	return func(_ context.Context, _ SubprocessConfig) (SubprocessResult, error) {
		return SubprocessResult{}, makeExitError137()
	}
}

// makeExitError137 produces a wrapped exit-137 error as spawnSubprocess would.
func makeExitError137() error {
	cmd := exec.Command("sh", "-c", "exit 137")
	err := cmd.Run()
	return fmt.Errorf("subprocess exited: %w", err)
}

// TestExecuteWithSplit_NoSplit verifies the happy path with no splitting needed.
func TestExecuteWithSplit_NoSplit(t *testing.T) {
	cat := &mockCatalog{meta: newMeta(1)}
	e, _ := newSplitEngine(cat)
	e.subprocessFn = successFn()

	c := makeCandidate(4)
	result, err := e.ExecuteJob(t.Context(), c)
	require.NoError(t, err)
	assert.Len(t, result.InputFiles, 4)
	assert.Equal(t, 1, cat.commitCalls)
}

// TestExecuteWithSplit_RecoversThenSucceeds verifies that an OOM on the full
// batch triggers a split and both halves succeed.
func TestExecuteWithSplit_RecoversThenSucceeds(t *testing.T) {
	cat := &mockCatalog{meta: newMeta(1)}
	e, _ := newSplitEngine(cat)
	// First call (4 files) OOMs; calls 2+3 (2 files each) succeed.
	e.subprocessFn = oomOnFirstN(1)

	c := makeCandidate(4)
	result, err := e.ExecuteJob(t.Context(), c)
	require.NoError(t, err)
	// Both halves committed independently.
	assert.Equal(t, 2, cat.commitCalls)
	// Merged result covers all 4 input files.
	assert.Len(t, result.InputFiles, 4)
	// OutputFile is empty for a merged result.
	assert.Empty(t, result.OutputFile)
}

// TestExecuteWithSplit_SingleFileOOM verifies that a 1-file candidate that OOMs
// returns an error without infinite recursion.
func TestExecuteWithSplit_SingleFileOOM(t *testing.T) {
	cat := &mockCatalog{meta: newMeta(1)}
	e, _ := newSplitEngine(cat)
	e.subprocessFn = alwaysOOM()

	c := makeCandidate(1)
	_, err := e.ExecuteJob(t.Context(), c)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot split further")
	assert.Equal(t, 0, cat.commitCalls)
}

// TestExecuteWithSplit_MaxDepth verifies that splitting stops at maxSplitDepth
// when all attempts OOM.
func TestExecuteWithSplit_MaxDepth(t *testing.T) {
	cat := &mockCatalog{meta: newMeta(1)}
	e, _ := newSplitEngine(cat)
	e.subprocessFn = alwaysOOM()

	// 2 files: can split once (depth 0→1), then hits 1-file limit.
	c := makeCandidate(2)
	_, err := e.ExecuteJob(t.Context(), c)
	require.Error(t, err)
	assert.Equal(t, 0, cat.commitCalls)
}

// TestExecuteWithSplit_NonRecoverableError verifies that a non-recoverable
// subprocess failure (exit 1) is returned immediately with no split attempt.
func TestExecuteWithSplit_NonRecoverableError(t *testing.T) {
	cat := &mockCatalog{meta: newMeta(1)}
	e, stor := newSplitEngine(cat)

	var calls atomic.Int32
	e.subprocessFn = func(_ context.Context, _ SubprocessConfig) (SubprocessResult, error) {
		calls.Add(1)
		cmd := exec.Command("sh", "-c", "exit 1")
		err := cmd.Run()
		return SubprocessResult{}, fmt.Errorf("subprocess exited: %w", err)
	}

	c := makeCandidate(4)
	_, err := e.ExecuteJob(t.Context(), c)
	require.Error(t, err)
	// Only one attempt — no split.
	assert.Equal(t, int32(1), calls.Load())
	assert.Equal(t, 0, cat.commitCalls)

	// Recovery manifest must be deleted after non-recoverable failure.
	keys, _ := stor.List(t.Context(), ".firn/recovery/")
	assert.Empty(t, keys, "recovery manifest should be deleted after non-recoverable failure")
}

// TestExecuteWithSplit_RecoveryManifestCleanedOnOOM verifies that the recovery
// manifest for a failed attempt is deleted before splitting.
func TestExecuteWithSplit_RecoveryManifestCleanedOnOOM(t *testing.T) {
	cat := &mockCatalog{meta: newMeta(1)}
	e, stor := newSplitEngine(cat)
	// OOM on first call, then succeed.
	e.subprocessFn = oomOnFirstN(1)

	c := makeCandidate(2)
	_, err := e.ExecuteJob(t.Context(), c)
	require.NoError(t, err)

	// After success, no recovery manifests should remain.
	keys, _ := stor.List(t.Context(), "")
	for _, k := range keys {
		assert.NotContains(t, k, ".firn/recovery/", "stale recovery manifest found: %s", k)
	}
}

// TestExecuteWithSplit_CommitConflictRetry verifies that commitWithRetry reloads
// the table on each conflict, updates ParentSnapshotID, and ultimately succeeds.
func TestExecuteWithSplit_CommitConflictRetry(t *testing.T) {
	// conflictCatalog fails first 2 commits then succeeds.
	cat := &conflictCatalog{meta: newMeta(1), failCount: 2}
	e, _ := newSplitEngine(cat)
	e.subprocessFn = successFn()

	_, err := e.ExecuteJob(t.Context(), makeCandidate(2))
	require.NoError(t, err)
	assert.Equal(t, 3, cat.commitCalls, "expected 2 conflict retries + 1 success")
}

// TestExecuteWithSplit_CommitConflictExhausted verifies that the error is
// propagated and wraps ErrConflict after all retry attempts are exhausted.
func TestExecuteWithSplit_CommitConflictExhausted(t *testing.T) {
	// Always conflict — exceeds the 3-attempt retryer used by newSplitEngine.
	cat := &conflictCatalog{meta: newMeta(1), failCount: 100}
	e, _ := newSplitEngine(cat)
	e.subprocessFn = successFn()

	_, err := e.ExecuteJob(t.Context(), makeCandidate(2))
	require.Error(t, err)
	var conflict catalog.ErrConflict
	assert.ErrorAs(t, err, &conflict, "expected wrapped ErrConflict")
	assert.Equal(t, 3, cat.commitCalls, "should stop after MaxAttempts")
}
