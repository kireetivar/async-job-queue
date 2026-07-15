package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	JobsEnqueued = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobs_enqueued_total",
		Help: "Total numbers of jobs enqueued",
	})

	JobsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobs_processed_total",
		Help: "Total number of jobs processed",
	})

	JobsFailed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobs_failed_total",
		Help: "Total number of failed jobs",
	})

	JobsRetried = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobs_retried_total",
		Help: "Total number of retried jobs",
	})

	JobsDead = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobs_dead_total",
		Help: "Total number of dead jobs",
	})

	WebhookRequests = promauto.NewCounter(prometheus.CounterOpts{
		Name: "webhook_requests_total",
		Help: "Total webhook HTTP calls made",
	})

	WebhookFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "webhook_failures_total",
		Help: "Webhook calls failed",
	})

	WebhookDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "webhook_duration_seconds",
		Help: "Time taken for webhook HTTP calls",
	})

	JobProcessingDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "jobs_processed_duration_seconds",
		Help: "Time taken for jobs to get processed in seconds",
	})

	ActiveWorkers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "active_workers",
		Help: "Current number of active worker goroutines",
	})
)
