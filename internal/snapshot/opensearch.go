package snapshot

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/SaiVyshnavi1522/dataplane-control-plane/internal/model"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type Config struct {
	EndpointTemplate string
	Bucket           string
	S3Endpoint       string
	Region           string
}

type OpenSearch struct {
	client           *http.Client
	endpointTemplate string
	bucket           string
	s3Endpoint       string
	region           string
}

func New(config Config) (*OpenSearch, error) {
	parsed, err := url.Parse(config.EndpointTemplate)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, fmt.Errorf("SNAPSHOT_ENDPOINT_TEMPLATE must be an HTTP(S) URL")
	}
	if config.Bucket == "" || config.S3Endpoint == "" || config.Region == "" {
		return nil, fmt.Errorf("snapshot bucket, S3 endpoint, and region are required")
	}
	return &OpenSearch{
		client:           &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)},
		endpointTemplate: strings.TrimRight(config.EndpointTemplate, "/"),
		bucket:           config.Bucket,
		s3Endpoint:       config.S3Endpoint,
		region:           config.Region,
	}, nil
}

func (o *OpenSearch) Create(ctx context.Context, cluster model.Cluster, backup model.Backup) error {
	if err := o.registerRepository(ctx, cluster); err != nil {
		return err
	}
	body := map[string]any{"indices": "*", "ignore_unavailable": true, "include_global_state": false}
	path := "/_snapshot/" + repositoryName(cluster) + "/" + url.PathEscape(backup.SnapshotName) + "?wait_for_completion=true"
	return o.request(ctx, cluster, http.MethodPut, path, body)
}

func (o *OpenSearch) Restore(ctx context.Context, cluster model.Cluster, backup model.Backup) error {
	if err := o.registerRepository(ctx, cluster); err != nil {
		return err
	}
	prefix := "restored-" + shortID(backup.ID) + "-"
	body := map[string]any{
		"indices":              "*",
		"ignore_unavailable":   true,
		"include_global_state": false,
		"rename_pattern":       "(.+)",
		"rename_replacement":   prefix + "$1",
	}
	path := "/_snapshot/" + repositoryName(cluster) + "/" + url.PathEscape(backup.SnapshotName) + "/_restore?wait_for_completion=true"
	return o.request(ctx, cluster, http.MethodPost, path, body)
}

func (o *OpenSearch) registerRepository(ctx context.Context, cluster model.Cluster) error {
	body := map[string]any{
		"type": "s3",
		"settings": map[string]any{
			"bucket":                      o.bucket,
			"base_path":                   "clusters/" + cluster.ID,
			"endpoint":                    o.s3Endpoint,
			"region":                      o.region,
			"protocol":                    "http",
			"path_style_access":           true,
			"server_side_encryption_type": "bucket_default",
		},
	}
	return o.request(ctx, cluster, http.MethodPut, "/_snapshot/"+repositoryName(cluster), body)
}

func (o *OpenSearch) request(ctx context.Context, cluster model.Cluster, method, path string, body any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	endpoint := strings.ReplaceAll(o.endpointTemplate, "{resource}", shortID(cluster.ID))
	request, err := http.NewRequestWithContext(ctx, method, endpoint+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := o.client.Do(request)
	if err != nil {
		return fmt.Errorf("OpenSearch snapshot request: %w", err)
	}
	defer response.Body.Close()
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if readErr != nil {
		return fmt.Errorf("read OpenSearch snapshot response: %w", readErr)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("OpenSearch snapshot request returned %s: %s", response.Status, strings.TrimSpace(string(responseBody)))
	}
	var result struct {
		Snapshot struct {
			State string `json:"state"`
		} `json:"snapshot"`
	}
	if len(responseBody) > 0 && json.Unmarshal(responseBody, &result) == nil && result.Snapshot.State != "" && result.Snapshot.State != "SUCCESS" {
		return fmt.Errorf("OpenSearch snapshot finished in %s state", result.Snapshot.State)
	}
	return nil
}

func repositoryName(cluster model.Cluster) string { return "dataplane-" + shortID(cluster.ID) }

func shortID(id string) string {
	id = strings.ToLower(id)
	if len(id) > 12 {
		return id[:12]
	}
	return id
}
