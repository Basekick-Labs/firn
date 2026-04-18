package iceberg

import "time"

// Manifest content type constants.
const (
	ManifestContentData    = 0
	ManifestContentDeletes = 1
)

// Manifest entry status constants.
const (
	EntryStatusExisting = 0
	EntryStatusAdded    = 1
	EntryStatusDeleted  = 2
)

// TableMetadata represents the contents of an Iceberg metadata.json file.
type TableMetadata struct {
	FormatVersion      int               `json:"format-version"`
	TableUUID          string            `json:"table-uuid"`
	Location           string            `json:"location"`
	LastSequenceNumber int64             `json:"last-sequence-number"`
	LastUpdatedMs      int64             `json:"last-updated-ms"`
	LastColumnID       int               `json:"last-column-id"`
	CurrentSnapshotID  int64             `json:"current-snapshot-id"`
	Snapshots          []Snapshot        `json:"snapshots"`
	Properties         map[string]string `json:"properties"`
}

// CurrentSnapshot returns the active snapshot, or nil if the table is empty.
func (m *TableMetadata) CurrentSnapshot() *Snapshot {
	for i := range m.Snapshots {
		if m.Snapshots[i].SnapshotID == m.CurrentSnapshotID {
			return &m.Snapshots[i]
		}
	}
	return nil
}

// SnapshotByID returns the snapshot with the given ID, or nil if not found.
func (m *TableMetadata) SnapshotByID(id int64) *Snapshot {
	for i := range m.Snapshots {
		if m.Snapshots[i].SnapshotID == id {
			return &m.Snapshots[i]
		}
	}
	return nil
}

// Snapshot represents one Iceberg snapshot.
type Snapshot struct {
	SnapshotID       int64             `json:"snapshot-id"`
	ParentSnapshotID int64             `json:"parent-snapshot-id,omitempty"`
	SequenceNumber   int64             `json:"sequence-number"`
	TimestampMs      int64             `json:"timestamp-ms"`
	ManifestList     string            `json:"manifest-list"`
	Summary          map[string]string `json:"summary,omitempty"`
}

func (s *Snapshot) Timestamp() time.Time {
	return time.UnixMilli(s.TimestampMs).UTC()
}

// ManifestFile is one entry in the manifest list (Avro OCF file).
type ManifestFile struct {
	Path               string `avro:"manifest_path"`
	Length             int64  `avro:"manifest_length"`
	PartitionSpecID    int32  `avro:"partition_spec_id"`
	Content            int    `avro:"content"` // ManifestContentData or ManifestContentDeletes
	SequenceNumber     int64  `avro:"sequence_number"`
	MinSequenceNumber  int64  `avro:"min_sequence_number"`
	AddedSnapshotID    int64  `avro:"added_snapshot_id"`
	AddedFilesCount    int32  `avro:"added_files_count"`
	ExistingFilesCount int32  `avro:"existing_files_count"`
	DeletedFilesCount  int32  `avro:"deleted_files_count"`
	AddedRowsCount     int64  `avro:"added_rows_count"`
	ExistingRowsCount  int64  `avro:"existing_rows_count"`
	DeletedRowsCount   int64  `avro:"deleted_rows_count"`
}

// ManifestEntry is one record in a manifest file (Avro OCF file).
// It wraps a DataFile with entry-level metadata.
type ManifestEntry struct {
	Status     int    `avro:"status"`      // EntryStatusExisting / Added / Deleted
	SnapshotID *int64 `avro:"snapshot_id"` // snapshot that added or deleted this file
	DataFile   DataFile `avro:"data_file"`
}

// DataFile holds the fields from a manifest entry needed for compaction
// candidate selection. Unknown Avro fields are silently ignored by hamba/avro.
type DataFile struct {
	Content       int    `avro:"content"`            // 0=data, 1=pos-deletes, 2=eq-deletes
	FilePath      string `avro:"file_path"`
	FileFormat    string `avro:"file_format"`
	RecordCount   int64  `avro:"record_count"`
	FileSizeBytes int64  `avro:"file_size_in_bytes"`
}
