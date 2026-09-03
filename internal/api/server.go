package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/example/dataplane-control-plane/internal/metrics"
	"github.com/example/dataplane-control-plane/internal/repository"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server struct {
	repo *repository.Repository
	mux  *http.ServeMux
}

type createClusterRequest struct {
	Name    string `json:"name"`
	Engine  string `json:"engine"`
	Version string `json:"version"`
	Nodes   int    `json:"nodes"`
}

type scaleRequest struct {
	Nodes int `json:"nodes"`
}

var namePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{2,39}$`)

func New(repo *repository.Repository) *Server {
	s := &Server{repo: repo, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler { return recoverMiddleware(metricsMiddleware(s.mux)) }

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	s.mux.HandleFunc("GET /readyz", s.ready)
	s.mux.Handle("GET /metrics", promhttp.Handler())
	s.mux.HandleFunc("POST /v1/clusters", s.createCluster)
	s.mux.HandleFunc("GET /v1/clusters", s.listClusters)
	s.mux.HandleFunc("GET /v1/clusters/{id}", s.getCluster)
	s.mux.HandleFunc("POST /v1/clusters/{id}/scale", s.scaleCluster)
	s.mux.HandleFunc("DELETE /v1/clusters/{id}", s.deleteCluster)
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.repo.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "DEPENDENCY_UNAVAILABLE", "database unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) createCluster(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" || len(key) > 128 {
		writeError(w, http.StatusBadRequest, "INVALID_IDEMPOTENCY_KEY", "Idempotency-Key header is required and must be <= 128 characters")
		return
	}
	var req createClusterRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		return
	}
	if err := validateCreate(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		return
	}
	cluster, reused, err := s.repo.CreateCluster(r.Context(), repository.CreateClusterInput{
		ID:             newID(),
		Name:           req.Name,
		Engine:         req.Engine,
		Version:        req.Version,
		DesiredNodes:   req.Nodes,
		IdempotencyKey: key,
	})
	if errors.Is(err, repository.ErrIdempotencyConflict) {
		writeError(w, http.StatusConflict, "IDEMPOTENCY_CONFLICT", err.Error())
		return
	}
	if err != nil {
		slog.Error("create cluster", "error", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "could not create cluster")
		return
	}
	status := http.StatusAccepted
	if reused {
		status = http.StatusOK
	}
	writeJSON(w, status, cluster)
}

func (s *Server) listClusters(w http.ResponseWriter, r *http.Request) {
	clusters, err := s.repo.ListClusters(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "could not list clusters")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": clusters})
}

func (s *Server) getCluster(w http.ResponseWriter, r *http.Request) {
	cluster, err := s.repo.GetCluster(r.Context(), r.PathValue("id"))
	if errors.Is(err, repository.ErrNotFound) {
		writeError(w, http.StatusNotFound, "CLUSTER_NOT_FOUND", "cluster not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "could not fetch cluster")
		return
	}
	writeJSON(w, http.StatusOK, cluster)
}

func (s *Server) scaleCluster(w http.ResponseWriter, r *http.Request) {
	var req scaleRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		return
	}
	if req.Nodes < 1 || req.Nodes > 3 {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "nodes must be between 1 and 3")
		return
	}
	cluster, err := s.repo.RequestScale(r.Context(), r.PathValue("id"), req.Nodes)
	if errors.Is(err, repository.ErrNotFound) {
		writeError(w, http.StatusNotFound, "CLUSTER_NOT_FOUND", "cluster not found")
		return
	}
	if errors.Is(err, repository.ErrInvalidTransition) {
		writeError(w, http.StatusConflict, "CLUSTER_STATE_CONFLICT", "cluster cannot be scaled from its current state")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "could not scale cluster")
		return
	}
	status := http.StatusOK
	if cluster.Status == "SCALING" {
		status = http.StatusAccepted
	}
	writeJSON(w, status, cluster)
}

func (s *Server) deleteCluster(w http.ResponseWriter, r *http.Request) {
	cluster, err := s.repo.RequestDelete(r.Context(), r.PathValue("id"))
	if errors.Is(err, repository.ErrNotFound) {
		writeError(w, http.StatusNotFound, "CLUSTER_NOT_FOUND", "cluster not found")
		return
	}
	if errors.Is(err, repository.ErrInvalidTransition) {
		writeError(w, http.StatusConflict, "CLUSTER_STATE_CONFLICT", "cluster cannot be deleted from its current state")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "could not delete cluster")
		return
	}
	status := http.StatusOK
	if cluster.Status == "DELETING" {
		status = http.StatusAccepted
	}
	writeJSON(w, status, cluster)
}

func validateCreate(req *createClusterRequest) error {
	req.Name = strings.ToLower(strings.TrimSpace(req.Name))
	if !namePattern.MatchString(req.Name) {
		return errors.New("name must be 3-40 lowercase letters, numbers, or hyphens and start with a letter")
	}
	if req.Engine == "" {
		req.Engine = "opensearch"
	}
	if req.Engine != "opensearch" {
		return errors.New("only opensearch is supported in v1")
	}
	if req.Version == "" {
		req.Version = "3.8.0"
	}
	if req.Version != "3.8.0" {
		return errors.New("v1 currently supports OpenSearch 3.8.0 only")
	}
	if req.Nodes == 0 {
		req.Nodes = 1
	}
	if req.Nodes < 1 || req.Nodes > 3 {
		return errors.New("nodes must be between 1 and 3")
	}
	return nil
}

func decodeJSON(r *http.Request, dst any) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return errors.New("invalid JSON request")
	}
	return nil
}

func newID() string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) { r.status = code; r.ResponseWriter.WriteHeader(code) }

func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		route := normalizedRoute(r)
		metrics.HTTPRequests.WithLabelValues(r.Method, route, strconv.Itoa(recorder.status)).Inc()
		metrics.HTTPDuration.WithLabelValues(r.Method, route).Observe(time.Since(start).Seconds())
	})
}

func normalizedRoute(r *http.Request) string {
	p := r.URL.Path
	if strings.HasPrefix(p, "/v1/clusters/") {
		if strings.HasSuffix(p, "/scale") {
			return "/v1/clusters/{id}/scale"
		}
		return "/v1/clusters/{id}"
	}
	return p
}

func recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				slog.Error("panic", "value", rec)
				writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
