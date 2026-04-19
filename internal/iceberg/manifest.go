package iceberg

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/basekick-labs/firn/internal/storage"
	"github.com/hamba/avro/v2/ocf"
)

// ReadManifestList reads the Avro OCF manifest list at the given URI and
// returns all data ManifestFile entries (delete manifests are excluded).
// Use ReadManifestListAll when delete manifests must also be included (e.g. orphan cleanup).
func ReadManifestList(ctx context.Context, stor storage.Backend, uri string) ([]ManifestFile, error) {
	all, err := ReadManifestListAll(ctx, stor, uri)
	if err != nil {
		return nil, err
	}
	results := all[:0]
	for _, mf := range all {
		if mf.Content != ManifestContentDeletes {
			results = append(results, mf)
		}
	}
	return results, nil
}

// ReadManifestListAll reads the Avro OCF manifest list at the given URI and
// returns every ManifestFile entry regardless of content type (data and delete
// manifests alike). Use this when building a complete live-file set, such as
// during orphan cleanup.
func ReadManifestListAll(ctx context.Context, stor storage.Backend, uri string) ([]ManifestFile, error) {
	path, err := URIToPath(uri)
	if err != nil {
		return nil, fmt.Errorf("manifest list URI %s: %w", uri, err)
	}

	rc, err := stor.Read(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("read manifest list %s: %w", path, err)
	}
	defer rc.Close()

	dec, err := ocf.NewDecoder(rc)
	if err != nil {
		return nil, fmt.Errorf("decode manifest list %s: %w", path, err)
	}

	var results []ManifestFile
	for dec.HasNext() {
		var mf ManifestFile
		if err := dec.Decode(&mf); err != nil {
			return nil, fmt.Errorf("decode manifest list entry %s: %w", path, err)
		}
		results = append(results, mf)
	}
	if err := dec.Error(); err != nil {
		return nil, fmt.Errorf("read manifest list %s: %w", path, err)
	}
	return results, nil
}

// ReadManifest reads an Avro OCF manifest file at the given URI and returns
// all non-deleted ManifestEntry records (status == EntryStatusDeleted excluded).
func ReadManifest(ctx context.Context, stor storage.Backend, uri string) ([]ManifestEntry, error) {
	path, err := URIToPath(uri)
	if err != nil {
		return nil, fmt.Errorf("manifest URI %s: %w", uri, err)
	}

	rc, err := stor.Read(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("read manifest %s: %w", path, err)
	}
	defer rc.Close()

	dec, err := ocf.NewDecoder(rc)
	if err != nil {
		return nil, fmt.Errorf("decode manifest %s: %w", path, err)
	}

	var results []ManifestEntry
	for dec.HasNext() {
		var entry ManifestEntry
		if err := dec.Decode(&entry); err != nil {
			return nil, fmt.Errorf("decode manifest entry %s: %w", path, err)
		}
		if entry.Status == EntryStatusDeleted {
			continue
		}
		results = append(results, entry)
	}
	if err := dec.Error(); err != nil {
		return nil, fmt.Errorf("read manifest %s: %w", path, err)
	}
	return results, nil
}

// URIToPath converts a full storage URI (s3://bucket/key) to a
// storage-backend-relative key (path component, leading slash stripped).
// Returns an error if the resulting key is empty.
func URIToPath(uri string) (string, error) {
	if !strings.Contains(uri, "://") {
		return uri, nil
	}
	u, err := url.Parse(uri)
	if err != nil {
		return "", err
	}
	// Strip leading slash — S3 keys don't start with /.
	p := strings.TrimPrefix(u.Path, "/")
	if p == "" {
		return "", fmt.Errorf("URI %q has empty path", uri)
	}
	return p, nil
}
