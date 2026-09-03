package metrics

import (
	"context"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

var (
	HTTPRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dataplane_http_requests_total",
		Help: "Total HTTP requests.",
	}, []string{"method", "route", "status"})
	HTTPDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dataplane_http_request_duration_seconds",
		Help:    "HTTP request duration.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "route"})
	JobsProcessed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dataplane_jobs_processed_total",
		Help: "Background jobs by type and outcome.",
	}, []string{"type", "outcome"})
	JobDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dataplane_job_duration_seconds",
		Help:    "Background job execution duration.",
		Buckets: []float64{0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120},
	}, []string{"type"})
	HTTPInFlight = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "dataplane_http_in_flight_requests",
		Help: "Current HTTP requests being served.",
	})
	GRPCRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dataplane_grpc_requests_total",
		Help: "Total gRPC requests.",
	}, []string{"method", "code"})
	GRPCDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dataplane_grpc_request_duration_seconds",
		Help:    "gRPC request duration.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method"})
	JobRetries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dataplane_job_retries_total",
		Help: "Lifecycle jobs rescheduled after a failed attempt.",
	}, []string{"type"})
	ActiveWorkers = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "dataplane_active_workers",
		Help: "Worker goroutines currently running.",
	})
)

func Register() {
	prometheus.MustRegister(HTTPRequests, HTTPDuration, JobsProcessed, JobDuration, HTTPInFlight, GRPCRequests, GRPCDuration, JobRetries, ActiveWorkers)
}

func GRPCUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		started := time.Now()
		response, err := handler(ctx, request)
		code := status.Code(err)
		GRPCRequests.WithLabelValues(info.FullMethod, code.String()).Inc()
		GRPCDuration.WithLabelValues(info.FullMethod).Observe(time.Since(started).Seconds())
		return response, err
	}
}

func ObserveHTTP(method, route string, code int, duration time.Duration) {
	HTTPRequests.WithLabelValues(method, route, strconv.Itoa(code)).Inc()
	HTTPDuration.WithLabelValues(method, route).Observe(duration.Seconds())
}
