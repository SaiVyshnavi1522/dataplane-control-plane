package snapshot

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/SaiVyshnavi1522/dataplane-control-plane/internal/model"
)

func TestOpenSearchCreatesAndRestoresS3Snapshot(t *testing.T) {
	requests := make([]string, 0, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests = append(requests, request.Method+" "+request.URL.RequestURI())
		if !strings.Contains(request.URL.Path, "/snapshot-") {
			var body struct {
				Settings map[string]any `json:"settings"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Errorf("decode repository body: %v", err)
			}
			if body.Settings["server_side_encryption_type"] != "bucket_default" || body.Settings["base_path"] != "clusters/01jordersabc1234567890123" {
				t.Errorf("repository settings=%v", body.Settings)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"snapshot":{"state":"SUCCESS"}}`))
	}))
	defer server.Close()

	snapshots, err := New(Config{EndpointTemplate: server.URL, Bucket: "dataplane-snapshots", S3Endpoint: "minio:9000", Region: "us-east-1"})
	if err != nil {
		t.Fatal(err)
	}
	cluster := model.Cluster{ID: "01jordersabc1234567890123"}
	backup := model.Backup{ID: "01jbackupabc12345678901234", SnapshotName: "snapshot-01jbackupabc12345678901234"}
	if err := snapshots.Create(context.Background(), cluster, backup); err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	if err := snapshots.Restore(context.Background(), cluster, backup); err != nil {
		t.Fatalf("restore snapshot: %v", err)
	}
	if len(requests) != 4 || !strings.Contains(requests[1], "wait_for_completion=true") || !strings.Contains(requests[3], "/_restore?wait_for_completion=true") {
		t.Fatalf("requests=%v", requests)
	}
}

func TestOpenSearchRejectsFailedSnapshotState(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if strings.Contains(request.URL.Path, "/snapshot-") {
			_, _ = w.Write([]byte(`{"snapshot":{"state":"FAILED"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"acknowledged":true}`))
	}))
	defer server.Close()
	snapshots, _ := New(Config{EndpointTemplate: server.URL, Bucket: "snapshots", S3Endpoint: "minio:9000", Region: "us-east-1"})
	err := snapshots.Create(context.Background(), model.Cluster{ID: "01jcatalog"}, model.Backup{SnapshotName: "snapshot-catalog"})
	if err == nil || !strings.Contains(err.Error(), "FAILED state") {
		t.Fatalf("error=%v, want failed state", err)
	}
}
