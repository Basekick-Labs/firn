package iceberg

import "time"

// TableMetadata represents the contents of an Iceberg metadata.json file.
type TableMetadata struct {
	FormatVersion      int        `json:"format-version"`
	TableUUID          string     `json:"table-uuid"`
	Location           string     `json:"location"`
	LastSequenceNumber int64      `json:"last-sequence-number"`
	LastUpdatedMs      int64      `json:"last-updated-ms"`
	LastColumnID       int        `json:"last-column-id"`
	CurrentSnapshotID  int64      `json:"current-snapshot-id"`
	Snapshots          []Snapshot `json:"snapshots"`
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

// ManifestFile is one entry in the manifest list (Avro file).
type ManifestFile struct {
	Path            string `avro:"manifest_path"`
	Length          int64  `avro:"manifest_length"`
	PartitionSpecID int    `avro:"partition_spec_id"`
	Content         int    `avro:"content"` // 0=data, 1=deletes
	SequenceNumber  int64  `avro:"sequence_number"`
	AddedFilesCount int    `avro:"added_files_count"`
	ExistingFilesCount int `avro:"existing_files_count"`
	DeletedFilesCount int  `avro:"deleted_files_count"`
	AddedRowsCount  int64  `avro:"added_rows_count"`
}

// DataFile is one entry in a manifest file (Avro file).
type DataFile struct {
	FilePath        string            `avro:"file_path"`
	FileFormat      string            `avro:"file_format"`
	PartitionData   map[string]any    `avro:"partition"`
	RecordCount     int64             `avro:"record_count"`
	FileSizeBytes   int64             `avro:"file_size_in_bytes"`
	ColumnSizes     map[int]int64     `avro:"column_sizes"`
	ValueCounts     map[int]int64     `avro:"value_counts"`
	NullValueCounts map[int]int64     `avro:"null_value_counts"`
}
