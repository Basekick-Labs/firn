// Package rest provides a shared Iceberg REST Catalog client used by the
// Lakekeeper, Polaris, and Nessie catalog implementations.
package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/basekick-labs/firn/internal/catalog"
	"github.com/basekick-labs/firn/internal/config"
	"github.com/basekick-labs/firn/internal/iceberg"
	"github.com/google/uuid"
)

// Client implements catalog.Client against any Iceberg REST Catalog server.
type Client struct {
	baseURL string
	http    *http.Client
	tokens  *tokenSource
}

// NewClient constructs a Client for an Iceberg REST Catalog.
//
//   - baseURL is the root of the REST API (e.g. "https://host/iceberg").
//     All Iceberg REST paths ("/v1/namespaces/...") are appended to it.
//   - tokenEndpoint is the OAuth2 token URL (e.g. "https://host/oauth/tokens").
//     Ignored when cred.ClientID is empty.
//   - cred supplies the OAuth2 client credentials. Pass a zero-value
//     config.CatalogCredential for unauthenticated access.
func NewClient(baseURL, tokenEndpoint string, cred config.CatalogCredential) *Client {
	c := &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    &http.Client{Timeout: 30 * time.Second},
	}
	if cred.ClientID != "" {
		c.tokens = newTokenSource(c.http, tokenEndpoint, cred.ClientID, cred.ClientSecret)
	}
	return c
}

// ListNamespaces returns all namespaces, paginating automatically.
func (c *Client) ListNamespaces(ctx context.Context) ([]string, error) {
	var result []string
	pageToken := ""

	for {
		path := "/v1/namespaces"
		if pageToken != "" {
			path += "?pageToken=" + url.QueryEscape(pageToken)
		}

		resp, err := c.do(ctx, http.MethodGet, path, nil)
		if err != nil {
			return nil, err
		}

		if err := checkStatus(resp, http.StatusOK); err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("ListNamespaces: %w", err)
		}

		var body struct {
			Namespaces    [][]string `json:"namespaces"`
			NextPageToken string     `json:"next-page-token"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("ListNamespaces decode: %w", err)
		}
		resp.Body.Close()

		for _, parts := range body.Namespaces {
			result = append(result, strings.Join(parts, "."))
		}

		if body.NextPageToken == "" {
			break
		}
		pageToken = body.NextPageToken
	}

	return result, nil
}

// ListTables returns all tables in a namespace, paginating automatically.
func (c *Client) ListTables(ctx context.Context, namespace string) ([]catalog.TableIdentifier, error) {
	var result []catalog.TableIdentifier
	pageToken := ""

	for {
		path := "/v1/namespaces/" + encodeNamespace(namespace) + "/tables"
		if pageToken != "" {
			path += "?pageToken=" + url.QueryEscape(pageToken)
		}

		resp, err := c.do(ctx, http.MethodGet, path, nil)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode == http.StatusNotFound {
			resp.Body.Close()
			return nil, nil
		}
		if err := checkStatus(resp, http.StatusOK); err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("ListTables %s: %w", namespace, err)
		}

		var body struct {
			Identifiers []struct {
				Namespace []string `json:"namespace"`
				Name      string   `json:"name"`
			} `json:"identifiers"`
			NextPageToken string `json:"next-page-token"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("ListTables decode: %w", err)
		}
		resp.Body.Close()

		for _, id := range body.Identifiers {
			result = append(result, catalog.TableIdentifier{
				Namespace: strings.Join(id.Namespace, "."),
				Name:      id.Name,
			})
		}

		if body.NextPageToken == "" {
			break
		}
		pageToken = body.NextPageToken
	}

	return result, nil
}

// LoadTable fetches the current Iceberg metadata for a table.
func (c *Client) LoadTable(ctx context.Context, id catalog.TableIdentifier) (*iceberg.TableMetadata, error) {
	path := "/v1/namespaces/" + encodeNamespace(id.Namespace) + "/tables/" + url.PathEscape(id.Name)

	resp, err := c.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrTableNotFound{Table: id}
	}
	if err := checkStatus(resp, http.StatusOK); err != nil {
		return nil, fmt.Errorf("LoadTable %s: %w", id, err)
	}

	var body struct {
		MetadataLocation string                `json:"metadata-location"`
		Metadata         iceberg.TableMetadata `json:"metadata"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("LoadTable decode: %w", err)
	}

	return &body.Metadata, nil
}

// CommitTransaction atomically commits a set of updates to a table.
// Returns catalog.ErrConflict on 409 (safe to retry).
// Accepts both 200 OK and 204 No Content as success — different Iceberg REST
// implementations vary in which status code they return.
func (c *Client) CommitTransaction(ctx context.Context, id catalog.TableIdentifier, tx catalog.Transaction) error {
	path := "/v1/namespaces/" + encodeNamespace(id.Namespace) + "/tables/" + url.PathEscape(id.Name) + "/transactions/commit"

	body, err := json.Marshal(marshalTransaction(tx))
	if err != nil {
		return fmt.Errorf("CommitTransaction marshal: %w", err)
	}

	resp, err := c.do(ctx, http.MethodPost, path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		return catalog.ErrConflict{Table: id}
	}
	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK {
		return nil
	}
	return fmt.Errorf("CommitTransaction %s: %w", id, statusError(resp))
}

// do executes an authenticated HTTP request.
func (c *Client) do(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Idempotency-Key", uuid.NewString())

	if c.tokens != nil {
		token, err := c.tokens.get(ctx)
		if err != nil {
			return nil, fmt.Errorf("get token: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}

	return c.http.Do(req)
}

// encodeNamespace converts a dot-separated namespace string into the Iceberg
// REST URL path segment using the ASCII unit separator (0x1F) per spec.
func encodeNamespace(ns string) string {
	parts := strings.Split(ns, ".")
	return url.PathEscape(strings.Join(parts, "\x1F"))
}

func checkStatus(resp *http.Response, want int) error {
	if resp.StatusCode == want {
		return nil
	}
	return statusError(resp)
}

func statusError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
}

// marshalTransaction converts catalog.Transaction to the Iceberg REST JSON shape.
func marshalTransaction(tx catalog.Transaction) any {
	type requirementJSON struct {
		Type       string `json:"type"`
		SnapshotID int64  `json:"snapshot-id,omitempty"`
	}
	type snapshotJSON struct {
		SnapshotID       int64             `json:"snapshot-id"`
		ParentSnapshotID int64             `json:"parent-snapshot-id,omitempty"`
		SequenceNumber   int64             `json:"sequence-number"`
		TimestampMs      int64             `json:"timestamp-ms"`
		ManifestList     string            `json:"manifest-list"`
		Summary          map[string]string `json:"summary,omitempty"`
	}
	type updateJSON struct {
		Type        string        `json:"type"`
		Snapshot    *snapshotJSON `json:"snapshot,omitempty"`
		RefName     string        `json:"ref-name,omitempty"`
		SnapshotID  int64         `json:"snapshot-id,omitempty"`
		SnapshotIDs []int64       `json:"snapshot-ids,omitempty"`
	}

	reqs := make([]requirementJSON, len(tx.Requirements))
	for i, r := range tx.Requirements {
		reqs[i] = requirementJSON{
			Type:       r.Type,
			SnapshotID: r.CurrentSnapshotID,
		}
	}

	upds := make([]updateJSON, len(tx.Updates))
	for i, u := range tx.Updates {
		upd := updateJSON{
			Type:        u.Type,
			RefName:     u.RefName,
			SnapshotIDs: u.SnapshotIDs,
		}
		if u.Snapshot != nil {
			upd.Snapshot = &snapshotJSON{
				SnapshotID:       u.Snapshot.SnapshotID,
				ParentSnapshotID: u.Snapshot.ParentSnapshotID,
				SequenceNumber:   u.Snapshot.SequenceNumber,
				TimestampMs:      u.Snapshot.TimestampMs,
				ManifestList:     u.Snapshot.ManifestList,
				Summary:          u.Snapshot.Summary,
			}
		}
		// set-snapshot-ref carries a bare snapshot-id (not a full snapshot object).
		if u.Type == "set-snapshot-ref" && u.Snapshot != nil {
			upd.SnapshotID = u.Snapshot.SnapshotID
		}
		upds[i] = upd
	}

	return struct {
		Requirements any `json:"requirements"`
		Updates      any `json:"updates"`
	}{reqs, upds}
}

// ErrTableNotFound is returned by LoadTable when a table does not exist.
type ErrTableNotFound struct {
	Table catalog.TableIdentifier
}

func (e ErrTableNotFound) Error() string {
	return "table not found: " + e.Table.String()
}

// tokenSource manages OAuth2 client credentials tokens with automatic refresh.
type tokenSource struct {
	mu           sync.Mutex
	httpClient   *http.Client
	tokenURL     string
	clientID     string
	clientSecret string
	token        string
	expiresAt    time.Time
}

func newTokenSource(hc *http.Client, tokenURL, clientID, clientSecret string) *tokenSource {
	return &tokenSource{
		httpClient:   hc,
		tokenURL:     tokenURL,
		clientID:     clientID,
		clientSecret: clientSecret,
	}
}

func (t *tokenSource) get(ctx context.Context) (string, error) {
	// Fast path: return cached token without doing I/O.
	t.mu.Lock()
	if t.token != "" && time.Now().Before(t.expiresAt) {
		tok := t.token
		t.mu.Unlock()
		return tok, nil
	}
	t.mu.Unlock()

	// Slow path: fetch a new token. The lock is NOT held during the HTTP call
	// so other goroutines are not blocked. A thundering herd on expiry is
	// acceptable here (extra token request); a singleflight group can be added
	// later if the token endpoint becomes a bottleneck.
	vals := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {t.clientID},
		"client_secret": {t.clientSecret},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.tokenURL,
		strings.NewReader(vals.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("token request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return "", fmt.Errorf("token request HTTP %d: %s", resp.StatusCode, body)
	}

	var tok struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tok); err != nil {
		return "", fmt.Errorf("token decode: %w", err)
	}

	// Write back under lock.
	t.mu.Lock()
	t.token = tok.AccessToken
	// Refresh 30s before actual expiry.
	t.expiresAt = time.Now().Add(time.Duration(tok.ExpiresIn)*time.Second - 30*time.Second)
	t.mu.Unlock()

	return tok.AccessToken, nil
}
