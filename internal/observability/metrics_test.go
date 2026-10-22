package observability_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability/obstest"
)

func TestRegister_PopulatesAllMetricVars(t *testing.T) {
	obstest.Init(t)

	require.NotNil(t, observability.TicketsMintedTotal)
	require.NotNil(t, observability.ConnectionsOpen)
	require.NotNil(t, observability.ConnectionsRejectedTotal)
	require.NotNil(t, observability.EventsIngestTotal)
	require.NotNil(t, observability.FramesWrittenTotal)
}

func TestRegister_SafeToCallTwice(t *testing.T) {
	obstest.Init(t)
	first := observability.TicketsMintedTotal

	assert.NotPanics(t, observability.Register)
	assert.Same(t, first, observability.TicketsMintedTotal)
}

func TestRegister_PreInitialisesClosedLabelSets(t *testing.T) {
	obstest.Init(t)

	for _, result := range []string{"ok", "rate_limited", "valkey_unavailable"} {
		assert.NotNil(t, observability.TicketsMintedTotal.WithLabelValues(result))
	}
	assert.Zero(t, obstest.Sum(t, "workflow_stream_gateway_connections_rejected_total", map[string]string{"reason": "cap_exceeded"}))
}
