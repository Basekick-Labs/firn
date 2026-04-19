package iceberg

// Avro OCF schema JSON strings for Iceberg manifest files.
// hamba/avro ignores fields present in the schema but absent from the Go struct,
// and ignores Go struct fields absent from the schema — safe to use minimal structs.

const manifestListAvroSchema = `{
	"type": "record",
	"name": "manifest_file",
	"fields": [
		{"name": "manifest_path",        "type": "string"},
		{"name": "manifest_length",       "type": "long"},
		{"name": "partition_spec_id",     "type": "int"},
		{"name": "content",               "type": "int"},
		{"name": "sequence_number",       "type": "long"},
		{"name": "min_sequence_number",   "type": "long"},
		{"name": "added_snapshot_id",     "type": "long"},
		{"name": "added_files_count",     "type": "int"},
		{"name": "existing_files_count",  "type": "int"},
		{"name": "deleted_files_count",   "type": "int"},
		{"name": "added_rows_count",      "type": "long"},
		{"name": "existing_rows_count",   "type": "long"},
		{"name": "deleted_rows_count",    "type": "long"}
	]
}`

const manifestEntryAvroSchema = `{
	"type": "record",
	"name": "manifest_entry",
	"fields": [
		{"name": "status",      "type": "int"},
		{"name": "snapshot_id", "type": ["null", "long"], "default": null},
		{"name": "data_file", "type": {
			"type": "record",
			"name": "r2",
			"fields": [
				{"name": "content",            "type": "int"},
				{"name": "file_path",          "type": "string"},
				{"name": "file_format",        "type": "string"},
				{"name": "record_count",       "type": "long"},
				{"name": "file_size_in_bytes", "type": "long"}
			]
		}}
	]
}`
