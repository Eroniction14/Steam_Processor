package metrics

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	MessagesReceived  prometheus.Counter
	MessagesProcessed prometheus.Counter
	MessagesEmitted   prometheus.Counter
	ErrorsTotal       prometheus.Counter
	RetriesTotal      prometheus.Counter
	DLQMessages       prometheus.Counter
	ProcessingLatency prometheus.Histogram
	ActiveWindows     prometheus.Gauge
}

func New(namespace string) *Metrics {
	m := &Metrics{
		MessagesReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_received_total",
			Help:      "Total messages consumed from Kafka",
		}),
		MessagesProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_processed_total",
			Help:      "Total messages successfully processed",
		}),
		MessagesEmitted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_emitted_total",
			Help:      "Total messages emitted to output topic",
		}),
		ErrorsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "errors_total",
			Help:      "Total processing errors",
		}),
		RetriesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "retries_total",
			Help:      "Total retry attempts",
		}),
		DLQMessages: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "dlq_messages_total",
			Help:      "Total messages sent to dead-letter queue",
		}),
		ProcessingLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "processing_latency_seconds",
			Help:      "Message processing latency",
			Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
		}),
		ActiveWindows: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "active_windows",
			Help:      "Number of active aggregation windows",
		}),
	}

	prometheus.MustRegister(
		m.MessagesReceived, m.MessagesProcessed, m.MessagesEmitted,
		m.ErrorsTotal, m.RetriesTotal, m.DLQMessages,
		m.ProcessingLatency, m.ActiveWindows,
	)

	return m
}

func (m *Metrics) ServeHTTP(port int) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(":"+strconv.Itoa(port), mux)
}
