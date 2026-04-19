package iceberg

import (
	"bytes"
	"context"
	"fmt"

	"github.com/basekick-labs/firn/internal/storage"
	"github.com/hamba/avro/v2/ocf"
)

// WriteManifest encodes entries as an Avro OCF manifest file and writes it to storage.
func WriteManifest(ctx context.Context, stor storage.Backend, path string, entries []ManifestEntry) error {
	data, err := encodeAvro(manifestEntryAvroSchema, func(enc *ocf.Encoder) error {
		for _, e := range entries {
			if err := enc.Encode(e); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("encode manifest %s: %w", path, err)
	}
	if err := stor.Write(ctx, path, bytes.NewReader(data), int64(len(data))); err != nil {
		return fmt.Errorf("write manifest %s: %w", path, err)
	}
	return nil
}

// WriteManifestList encodes files as an Avro OCF manifest list and writes it to storage.
func WriteManifestList(ctx context.Context, stor storage.Backend, path string, files []ManifestFile) error {
	data, err := encodeAvro(manifestListAvroSchema, func(enc *ocf.Encoder) error {
		for _, f := range files {
			if err := enc.Encode(f); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("encode manifest list %s: %w", path, err)
	}
	if err := stor.Write(ctx, path, bytes.NewReader(data), int64(len(data))); err != nil {
		return fmt.Errorf("write manifest list %s: %w", path, err)
	}
	return nil
}

func encodeAvro(schema string, fn func(*ocf.Encoder) error) ([]byte, error) {
	var buf bytes.Buffer
	enc, err := ocf.NewEncoder(schema, &buf)
	if err != nil {
		return nil, err
	}
	if err := fn(enc); err != nil {
		return nil, err
	}
	if err := enc.Flush(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
