package observability

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegister_PopulatesAllMetricVars(t *testing.T) {
	Register()

	require.NotNil(t, TicketsMintedTotal)
	require.NotNil(t, ConnectionsOpen)
	require.NotNil(t, ConnectionsRejectedTotal)
	require.NotNil(t, EventsIngestTotal)
	require.NotNil(t, FramesWrittenTotal)
}

func TestRegister_SafeToCallTwice(t *testing.T) {
	Register()
	first := TicketsMintedTotal

	assert.NotPanics(t, Register)
	assert.Same(t, first, TicketsMintedTotal)
}
