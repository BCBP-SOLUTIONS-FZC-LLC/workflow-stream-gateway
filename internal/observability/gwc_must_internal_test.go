package observability

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
)

// A constructor error becomes a panic that names the cause after a fixed prefix.
func TestGwCMust_ThePanicMessageNamesTheConstructorError(t *testing.T) {
	assert.PanicsWithValue(t, "metrics: register service collector: name rejected", func() {
		must(0, errors.New("name rejected"))
	})
}

// Registering the service collectors a second time clashes with the first
// registration on the real registry: the panic carries the prefix and the
// registry's own message, and the collectors already in use are not replaced.
func TestGwCRegisterServiceMetrics_ARegistryClashPanicsWithThePrefixAndKeepsTheFirstCollectors(t *testing.T) {
	_ = gincommon.ObservabilityMiddlewares(gincommon.Config{
		ServiceName:  ServiceName,
		BuildVersion: "test",
		Domain:       Domain,
		Environment:  "test",
	})
	Register()
	firstTickets, firstOpen := TicketsMintedTotal, ConnectionsOpen
	require.NotNil(t, firstTickets)

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		registerServiceMetrics()
	}()

	message, isString := recovered.(string)
	require.True(t, isString, "expected a string panic, got %#v", recovered)
	assert.True(t, strings.HasPrefix(message, "metrics: register service collector: "), "got %q", message)
	assert.Contains(t, message, "duplicate metrics collector registration attempted")
	assert.Same(t, firstTickets, TicketsMintedTotal, "the collector in use was not replaced")
	assert.Same(t, firstOpen, ConnectionsOpen)
}
