// Package observability centralizes this service's Prometheus metrics
// (LLD §11) — one Register() for every metric, not scattered promauto
// calls per package, mirroring execution_service's own
// internal/observability convention.
package observability

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
)

// Metric vars are nil until Register runs — callers outside this package
// must nil-guard before use, since Register is invoked once at real
// process boot (cmd/server's job, not this package's).
var (
	TicketsMintedTotal       *prometheus.CounterVec
	ConnectionsOpen          prometheus.Gauge
	ConnectionsRejectedTotal *prometheus.CounterVec
	EventsIngestTotal        *prometheus.CounterVec
	FramesWrittenTotal       *prometheus.CounterVec
)

var registerOnce sync.Once

// Register constructs and registers every metric above against the
// default Prometheus registry. Safe to call more than once — only the
// first call has any effect.
func Register() {
	registerOnce.Do(register)
}

func register() {
	TicketsMintedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sse_tickets_minted_total",
		Help: "Total POST /api/v1/stream/tickets calls, by result.",
	}, []string{"result"})

	ConnectionsOpen = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sse_connections_open",
		Help: "Current open SSE connection count — the scaling signal for this service (LLD §10.2).",
	})

	ConnectionsRejectedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sse_connections_rejected_total",
		Help: "Total GET /api/v1/stream connections rejected, by reason.",
	}, []string{"reason"})

	// event_type is open-ended in spirit despite today's 12-value
	// catalogue (matches execution_service's own precedent for its
	// identically-shaped internal_events_ingest_total) — not pre-inited.
	EventsIngestTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "internal_events_ingest_total",
		Help: "Total POST /internal/events calls, by event_type and result.",
	}, []string{"event_type", "result"})

	FramesWrittenTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sse_frames_written_total",
		Help: "Total Valkey Stream entries written by the fan-out path, by event_type.",
	}, []string{"event_type"})

	// gincommon.MetricsRegisterer(), not prometheus.DefaultRegisterer
	// directly — keeps this package's collectors on the same registerer as
	// gincommon's own HTTP metrics (its own doc comment's stated reason),
	// which today is DefaultRegisterer but isn't guaranteed to stay so.
	gincommon.MetricsRegisterer().MustRegister(
		TicketsMintedTotal,
		ConnectionsOpen,
		ConnectionsRejectedTotal,
		EventsIngestTotal,
		FramesWrittenTotal,
	)

	preInitializeClosedLabelSets()
}

// preInitializeClosedLabelSets pre-creates label-combination children for
// metrics whose every label's vocabulary is fixed by the LLD, so
// dashboards show 0 instead of "no data" immediately after deploy.
func preInitializeClosedLabelSets() {
	for _, result := range []string{"ok", "rate_limited", "valkey_unavailable"} {
		TicketsMintedTotal.WithLabelValues(result)
	}
	for _, reason := range []string{"cap_exceeded", "invalid_ticket", "valkey_unavailable"} {
		ConnectionsRejectedTotal.WithLabelValues(reason)
	}
}
