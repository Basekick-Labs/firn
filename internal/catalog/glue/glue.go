package glue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/basekick-labs/firn/internal/catalog"
	"github.com/basekick-labs/firn/internal/config"
	"github.com/basekick-labs/firn/internal/iceberg"
	"github.com/google/uuid"
)

const (
	// metadataLocationKey is the Glue table parameter that holds the S3 URI
	// of the current Iceberg metadata.json file.
	metadataLocationKey = "metadata_location"

	tableTypeKey   = "table_type"
	tableTypeValue = "ICEBERG"
)

// Client implements catalog.Client against AWS Glue Data Catalog.
//
// Iceberg metadata is stored directly in S3; Glue holds only the pointer
// (metadata_location table parameter). CommitTransaction:
//  1. Reads the current metadata.json from S3
//  2. Validates transaction requirements (optimistic snapshot-ID check)
//  3. Applies updates and writes a new metadata.json to S3
//  4. Calls Glue UpdateTable to advance the metadata_location pointer
//
// Safety note: Glue UpdateTable does not support conditional writes. Single-
// instance Firn is safe (the per-table mutex prevents in-process races). Running
// multiple Firn instances or alongside another Iceberg writer against the same
// Glue table is NOT safe — the last UpdateTable call wins silently.
type Client struct {
	glue    *awsglue.Client
	s3      *s3.Client
	mu      sync.Mutex
	tableMu map[string]*sync.Mutex
}

func New(ctx context.Context, cfg config.CatalogConfig) (*Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithHTTPClient(&http.Client{Timeout: 30 * time.Second}),
	}
	if cfg.Credential.ClientID != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.Credential.ClientID, cfg.Credential.ClientSecret, ""),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("glue: load aws config: %w", err)
	}

	return &Client{
		glue:    awsglue.NewFromConfig(awsCfg),
		s3:      s3.NewFromConfig(awsCfg),
		tableMu: make(map[string]*sync.Mutex),
	}, nil
}

// tableLock returns a per-table mutex, creating it if necessary.
func (c *Client) tableLock(id catalog.TableIdentifier) *sync.Mutex {
	key := id.String()
	c.mu.Lock()
	m, ok := c.tableMu[key]
	if !ok {
		m = &sync.Mutex{}
		c.tableMu[key] = m
	}
	c.mu.Unlock()
	return m
}

// ListNamespaces returns all Glue databases.
func (c *Client) ListNamespaces(ctx context.Context) ([]string, error) {
	var result []string
	var nextToken *string

	for {
		out, err := c.glue.GetDatabases(ctx, &awsglue.GetDatabasesInput{
			NextToken: nextToken,
		})
		if err != nil {
			return nil, fmt.Errorf("glue ListNamespaces: %w", err)
		}

		for _, db := range out.DatabaseList {
			result = append(result, aws.ToString(db.Name))
		}

		if out.NextToken == nil {
			break
		}
		nextToken = out.NextToken
	}

	return result, nil
}

// ListTables returns all Iceberg tables in the given Glue database.
func (c *Client) ListTables(ctx context.Context, namespace string) ([]catalog.TableIdentifier, error) {
	var result []catalog.TableIdentifier
	var nextToken *string

	for {
		out, err := c.glue.GetTables(ctx, &awsglue.GetTablesInput{
			DatabaseName: aws.String(namespace),
			NextToken:    nextToken,
		})
		if err != nil {
			var notFound *types.EntityNotFoundException
			if errors.As(err, &notFound) {
				return nil, nil
			}
			return nil, fmt.Errorf("glue ListTables %s: %w", namespace, err)
		}

		for _, t := range out.TableList {
			if !isIcebergTable(t.Parameters) {
				continue
			}
			result = append(result, catalog.TableIdentifier{
				Namespace: namespace,
				Name:      aws.ToString(t.Name),
			})
		}

		if out.NextToken == nil {
			break
		}
		nextToken = out.NextToken
	}

	return result, nil
}

// LoadTable fetches the Iceberg metadata for a table by reading the
// metadata.json file whose path is stored in the Glue table parameters.
func (c *Client) LoadTable(ctx context.Context, id catalog.TableIdentifier) (*iceberg.TableMetadata, error) {
	out, err := c.glue.GetTable(ctx, &awsglue.GetTableInput{
		DatabaseName: aws.String(id.Namespace),
		Name:         aws.String(id.Name),
	})
	if err != nil {
		var notFound *types.EntityNotFoundException
		if errors.As(err, &notFound) {
			return nil, ErrTableNotFound{Table: id}
		}
		return nil, fmt.Errorf("glue LoadTable %s: %w", id, err)
	}

	metaLoc, ok := out.Table.Parameters[metadataLocationKey]
	if !ok || metaLoc == "" {
		return nil, fmt.Errorf("glue LoadTable %s: missing %s parameter", id, metadataLocationKey)
	}

	meta, err := c.readMetadata(ctx, metaLoc)
	if err != nil {
		return nil, fmt.Errorf("glue LoadTable %s: %w", id, err)
	}
	return meta, nil
}

// CommitTransaction validates requirements, applies updates, writes new
// metadata.json to S3, and advances the Glue metadata_location pointer.
// Returns catalog.ErrConflict if requirements are not satisfied or Glue
// reports a concurrent modification.
//
// A per-table mutex prevents in-process races. Cross-process safety requires
// a single Firn instance per set of tables (Glue UpdateTable is not conditional).
func (c *Client) CommitTransaction(ctx context.Context, id catalog.TableIdentifier, tx catalog.Transaction) error {
	lock := c.tableLock(id)
	lock.Lock()
	defer lock.Unlock()

	out, err := c.glue.GetTable(ctx, &awsglue.GetTableInput{
		DatabaseName: aws.String(id.Namespace),
		Name:         aws.String(id.Name),
	})
	if err != nil {
		return fmt.Errorf("glue CommitTransaction %s: %w", id, err)
	}

	currentMetaLoc := out.Table.Parameters[metadataLocationKey]
	meta, err := c.readMetadata(ctx, currentMetaLoc)
	if err != nil {
		return fmt.Errorf("glue CommitTransaction %s: read metadata: %w", id, err)
	}

	// Validate requirements — mismatch → ErrConflict.
	for _, req := range tx.Requirements {
		if err := checkRequirement(req, meta); err != nil {
			return catalog.ErrConflict{Table: id}
		}
	}

	newMeta, err := applyUpdates(meta, tx.Updates)
	if err != nil {
		return fmt.Errorf("glue CommitTransaction %s: apply updates: %w", id, err)
	}

	newMetaLoc, err := nextMetadataLocation(currentMetaLoc)
	if err != nil {
		return fmt.Errorf("glue CommitTransaction %s: metadata path: %w", id, err)
	}

	if err := c.writeMetadata(ctx, newMetaLoc, newMeta); err != nil {
		return fmt.Errorf("glue CommitTransaction %s: write metadata: %w", id, err)
	}

	// Advance Glue pointer. On failure, attempt a best-effort S3 delete of
	// the new metadata file to avoid accumulating orphans. The delete is
	// best-effort only — orphan cleanup handles any residual files.
	params := cloneParams(out.Table.Parameters)
	params[metadataLocationKey] = newMetaLoc

	_, updateErr := c.glue.UpdateTable(ctx, &awsglue.UpdateTableInput{
		DatabaseName: aws.String(id.Namespace),
		TableInput: &types.TableInput{
			Name:              out.Table.Name,
			Description:       out.Table.Description,
			Owner:             out.Table.Owner,
			Retention:         out.Table.Retention,
			StorageDescriptor: out.Table.StorageDescriptor,
			PartitionKeys:     out.Table.PartitionKeys,
			TableType:         out.Table.TableType,
			Parameters:        params,
			TargetTable:       out.Table.TargetTable,
		},
	})
	if updateErr != nil {
		_ = c.deleteObject(ctx, newMetaLoc) // best-effort orphan cleanup
		var concurrentMod *types.ConcurrentModificationException
		if errors.As(updateErr, &concurrentMod) {
			return catalog.ErrConflict{Table: id}
		}
		return fmt.Errorf("glue CommitTransaction %s: update table: %w", id, updateErr)
	}

	return nil
}

// readMetadata fetches and parses an Iceberg metadata.json from S3.
func (c *Client) readMetadata(ctx context.Context, uri string) (*iceberg.TableMetadata, error) {
	bucket, key, err := parseS3URI(uri)
	if err != nil {
		return nil, fmt.Errorf("parse metadata URI %q: %w", uri, err)
	}

	obj, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 get %s: %w", uri, err)
	}
	defer obj.Body.Close()

	var meta iceberg.TableMetadata
	if err := json.NewDecoder(obj.Body).Decode(&meta); err != nil {
		return nil, fmt.Errorf("decode metadata %s: %w", uri, err)
	}
	return &meta, nil
}

// writeMetadata serialises metadata and uploads it to S3.
func (c *Client) writeMetadata(ctx context.Context, uri string, meta *iceberg.TableMetadata) error {
	bucket, key, err := parseS3URI(uri)
	if err != nil {
		return fmt.Errorf("parse metadata URI %q: %w", uri, err)
	}

	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	_, err = c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
		ContentType:   aws.String("application/json"),
	})
	return err
}

// deleteObject is a best-effort S3 delete used to clean up orphaned metadata files.
func (c *Client) deleteObject(ctx context.Context, uri string) error {
	bucket, key, err := parseS3URI(uri)
	if err != nil {
		return err
	}
	_, err = c.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return err
}

// checkRequirement validates a single transaction requirement against current metadata.
func checkRequirement(req catalog.Requirement, meta *iceberg.TableMetadata) error {
	switch req.Type {
	case "assert-current-snapshot-id":
		if meta.CurrentSnapshotID != req.CurrentSnapshotID {
			return fmt.Errorf("snapshot ID mismatch: want %d, got %d",
				req.CurrentSnapshotID, meta.CurrentSnapshotID)
		}
	default:
		return fmt.Errorf("unsupported requirement type: %q", req.Type)
	}
	return nil
}

// applyUpdates returns a new TableMetadata with the transaction updates applied.
// The original metadata is not mutated.
func applyUpdates(meta *iceberg.TableMetadata, updates []catalog.Update) (*iceberg.TableMetadata, error) {
	newMeta := *meta
	newMeta.LastUpdatedMs = time.Now().UnixMilli()

	for _, u := range updates {
		switch u.Type {
		case "add-snapshot":
			if u.Snapshot == nil {
				return nil, fmt.Errorf("add-snapshot update missing snapshot")
			}
			newMeta.Snapshots = append(append([]iceberg.Snapshot{}, newMeta.Snapshots...), *u.Snapshot)
			if u.Snapshot.SequenceNumber > newMeta.LastSequenceNumber {
				newMeta.LastSequenceNumber = u.Snapshot.SequenceNumber
			}

		case "set-snapshot-ref":
			if u.RefName == "main" && len(u.SnapshotIDs) > 0 {
				newMeta.CurrentSnapshotID = u.SnapshotIDs[0]
			}

		case "remove-snapshots":
			remove := make(map[int64]bool, len(u.SnapshotIDs))
			for _, id := range u.SnapshotIDs {
				remove[id] = true
			}
			keep := make([]iceberg.Snapshot, 0, len(newMeta.Snapshots))
			for _, s := range newMeta.Snapshots {
				if !remove[s.SnapshotID] {
					keep = append(keep, s)
				}
			}
			newMeta.Snapshots = keep
		}
	}

	return &newMeta, nil
}

// nextMetadataLocation derives the S3 URI for the new metadata file.
// Uses a UUID filename to guarantee uniqueness across retries and concurrent writers.
func nextMetadataLocation(currentLoc string) (string, error) {
	slash := strings.LastIndex(currentLoc, "/")
	if slash < 0 {
		return "", fmt.Errorf("cannot determine metadata directory from %q", currentLoc)
	}
	dir := currentLoc[:slash]
	return dir + "/" + uuid.New().String() + ".metadata.json", nil
}

// parseS3URI splits s3://bucket/key into (bucket, key).
func parseS3URI(uri string) (bucket, key string, err error) {
	if !strings.HasPrefix(uri, "s3://") {
		return "", "", fmt.Errorf("not an s3 URI: %q", uri)
	}
	rest := uri[len("s3://"):]
	slash := strings.Index(rest, "/")
	if slash < 0 {
		return "", "", fmt.Errorf("no key in s3 URI: %q", uri)
	}
	return rest[:slash], rest[slash+1:], nil
}

func isIcebergTable(params map[string]string) bool {
	v, ok := params[tableTypeKey]
	return ok && strings.EqualFold(v, tableTypeValue)
}

func cloneParams(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// ErrTableNotFound is returned when a Glue table does not exist.
type ErrTableNotFound struct {
	Table catalog.TableIdentifier
}

func (e ErrTableNotFound) Error() string {
	return "glue: table not found: " + e.Table.String()
}
