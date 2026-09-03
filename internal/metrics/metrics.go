package metrics

import "github.com/prometheus/client_golang/prometheus"

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
)

func Register() {
	prometheus.MustRegister(HTTPRequests, HTTPDuration, JobsProcessed, JobDuration)
}
