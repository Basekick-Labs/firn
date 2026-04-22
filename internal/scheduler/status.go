package scheduler

import "time"

// CycleStatus is the result of the most recently completed maintenance cycle.
type CycleStatus struct {
	StartedAt  time.Time     `json:"started_at"`
	FinishedAt time.Time     `json:"finished_at"`
	Duration   string        `json:"duration"`
	Tables     []TableStatus `json:"tables"`
}

// TableStatus summarises what happened to one table in the last cycle.
type TableStatus struct {
	Table      string            `json:"table"`
	Compaction *CompactionStatus `json:"compaction,omitempty"`
	Expiry     *ExpiryStatus     `json:"expiry,omitempty"`
	Orphan     *OrphanStatus     `json:"orphan,omitempty"`
}

// CompactionStatus aggregates all compaction jobs run for a table in one cycle.
type CompactionStatus struct {
	Jobs        int   `json:"jobs"`
	FilesMerged int   `json:"files_merged"`
	BytesBefore int64 `json:"bytes_before"`
	BytesAfter  int64 `json:"bytes_after"`
	Errors      int   `json:"errors"`
}

// ExpiryStatus summarises snapshot expiry for one table in one cycle.
type ExpiryStatus struct {
	ExpiredSnapshots int    `json:"expired_snapshots"`
	DeletedManifests int    `json:"deleted_manifests"`
	DeletedDataFiles int    `json:"deleted_data_files"`
	Error            string `json:"error,omitempty"`
}

// OrphanStatus summarises orphan file cleanup for one table in one cycle.
type OrphanStatus struct {
	ScannedFiles int    `json:"scanned_files"`
	DeletedFiles int    `json:"deleted_files"`
	SkippedFiles int    `json:"skipped_files"`
	Error        string `json:"error,omitempty"`
}
