package lib

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"sync"
)

var (
	ErrorCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "nirn_proxy_error",
		Help: "The total number of errors when processing requests",
	})

	RequestHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "nirn_proxy_requests",
		Help:        "Request histogram",
		Buckets: 	 []float64{.1, .25, 1, 2.5, 5, 20},
	}, []string{"method", "status", "route", "clientId"})

	ConnectionsOpen = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "nirn_proxy_open_connections",
		Help:        "Gauge for client connections currently open with the proxy",
	}, []string{"method", "route"})

	RequestsRoutedSent = promauto.NewCounter(prometheus.CounterOpts{
		Name:		"nirn_proxy_requests_routed_sent",
		Help:		"Counter for requests routed from this node into other nodes",
	})

	RequestsRoutedRecv = promauto.NewCounter(prometheus.CounterOpts{
		Name:		"nirn_proxy_requests_routed_received",
		Help:		"Counter for requests received from other nodes",
	})

	RequestsRoutedError = promauto.NewCounter(prometheus.CounterOpts{
		Name:		"nirn_proxy_requests_routed_error",
		Help:		"Counter for failed requests routed from this node",
	})

	GlobalWaitHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "nirn_proxy_global_wait_seconds",
		Help:    "Time spent waiting for a global token, by priority and outcome (granted, shed, cancelled)",
		Buckets: []float64{.005, .05, .25, 1, 2.5, 5, 10, 20, 30, 60},
	}, []string{"priority", "outcome"})

	QueueWaitHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "nirn_proxy_queue_wait_seconds",
		Help:    "Time from arrival to being sent to Discord (or dropped), by priority and outcome",
		Buckets: []float64{.005, .05, .25, 1, 2.5, 5, 10, 20, 30, 60, 120},
	}, []string{"priority", "outcome"})

	ShedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "nirn_proxy_shed_total",
		Help: "Requests dropped by the proxy before being sent to Discord, by reason and priority",
	}, []string{"reason", "priority"})

	CancelledBeforeSendCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "nirn_proxy_cancelled_before_send_total",
		Help: "Requests whose client gave up before the proxy sent them; no token was spent on them",
	}, []string{"priority"})

	Global429Counter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "nirn_proxy_global_429_total",
		Help: "Global 429s received from Discord. Any increase means the configured global limit is above Discord's",
	})
)

var registerWaitingGauges sync.Once

// RegisterGlobalWaitingGauges exports how many callers are queued for a
// global token, per priority. Once per process.
func RegisterGlobalWaitingGauges(source func() [priorityCount]int) {
	registerWaitingGauges.Do(func() {
		for p := Priority(0); p < priorityCount; p++ {
			p := p
			promauto.NewGaugeFunc(prometheus.GaugeOpts{
				Name:        "nirn_proxy_global_waiting",
				Help:        "Requests currently waiting for a global token",
				ConstLabels: prometheus.Labels{"priority": p.String()},
			}, func() float64 {
				return float64(source()[p])
			})
		}
	})
}

func StartMetrics(addr string) {
	prometheus.MustRegister(RequestHistogram)
	http.Handle("/metrics", promhttp.Handler())
	logger.Info("Starting metrics server on " + addr)
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		logger.Error(err)
		return
	}
}