package observability

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
)

const (
	Domain      = "workflow"
	ServiceName = "workflow-stream-gateway"
)

var (
	TicketsMintedTotal       *prometheus.CounterVec
	ConnectionsOpen          prometheus.Gauge
	ConnectionsRejectedTotal *prometheus.CounterVec
	EventsIngestTotal        *prometheus.CounterVec
	FramesWrittenTotal       *prometheus.CounterVec
)

var registerOnce sync.Once

func Register() {
	registerOnce.Do(registerServiceMetrics)
}

func must[C any](c C, err error) C {
	if err != nil {
		panic("metrics: register service collector: " + err.Error())
	}
	return c
}

func registerServiceMetrics() {
	TicketsMintedTotal = must(gincommon.NewCounterVec(nil,
		"workflow_stream_gateway_tickets_minted_total",
		"Total ticket mint calls, by result.",
		"result"))
	ConnectionsOpen = must(gincommon.NewGauge(nil,
		"workflow_stream_gateway_connections_open",
		"Current open SSE connection count, the scaling signal for this service."))
	ConnectionsRejectedTotal = must(gincommon.NewCounterVec(nil,
		"workflow_stream_gateway_connections_rejected_total",
		"Total stream connections rejected, by reason.",
		"reason"))
	EventsIngestTotal = must(gincommon.NewCounterVec(nil,
		"workflow_stream_gateway_internal_events_ingest_total",
		"Total internal event ingest calls, by event_type and result.",
		"event_type", "result"))
	FramesWrittenTotal = must(gincommon.NewCounterVec(nil,
		"workflow_stream_gateway_frames_written_total",
		"Total Valkey Stream entries written by the fan-out path, by event_type.",
		"event_type"))

	preInitializeClosedLabelSets()
}

func preInitializeClosedLabelSets() {
	for _, result := range []string{"ok", "rate_limited", "valkey_unavailable"} {
		TicketsMintedTotal.WithLabelValues(result)
	}
	for _, reason := range []string{"cap_exceeded", "invalid_ticket", "valkey_unavailable"} {
		ConnectionsRejectedTotal.WithLabelValues(reason)
	}
}
