package observability_test

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/logger"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability/obstest"
)

// gwCLog records nothing; it is a distinct value so a test can see that
// GinConfig hands the same logger on.
type gwCLog struct{ id string }

func (*gwCLog) Debug(string, map[string]interface{}) {}
func (*gwCLog) Info(string, map[string]interface{})  {}
func (*gwCLog) Warn(string, map[string]interface{})  {}
func (*gwCLog) Error(string, map[string]interface{}) {}

// gwCFreshProcess runs the calling test once more in a new process, where no
// other test has registered a metric yet, and fails the caller if that run
// fails. It returns true inside that new process. Registration happens once per
// process, so this is the only way to see what Register itself creates.
func gwCFreshProcess(t *testing.T) (inFreshProcess bool) {
	t.Helper()
	const marker = "GWC_FRESH_PROCESS"
	if os.Getenv(marker) == t.Name() {
		return true
	}
	cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$", "-test.count=1")
	cmd.Env = append(os.Environ(), marker+"="+t.Name())
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "the fresh-process run failed:\n%s", out)
	return false
}

// gwCSeries gathers the registry and returns, for one metric family, the value
// of every series keyed by the given label.
func gwCSeries(t *testing.T, family, label string) map[string]float64 {
	t.Helper()
	gathered, err := gincommon.MetricsGatherer().Gather()
	require.NoError(t, err)
	out := map[string]float64{}
	for _, mf := range gathered {
		if mf.GetName() != family {
			continue
		}
		for _, m := range mf.GetMetric() {
			key := ""
			for _, lp := range m.GetLabel() {
				if lp.GetName() == label {
					key = lp.GetValue()
				}
			}
			_, duplicate := out[key]
			require.False(t, duplicate, "two series share %s=%q", label, key)
			out[key] = m.GetCounter().GetValue()
		}
	}
	return out
}

// GinConfig passes the identity and the logger through unchanged and always
// names the workflow domain.
func TestGwCGinConfig_CopiesTheCallersFieldsAndFixesTheDomain(t *testing.T) {
	log := &gwCLog{id: "the logger"}
	cases := []struct {
		name                              string
		serviceName, version, environment string
	}{
		{"production values", "workflow-stream-gateway", "v1.4.2", "production"},
		{"other values", "another-service", "abc123", "staging"},
		{"empty values are copied, not defaulted", "", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := observability.GinConfig(tc.serviceName, tc.version, tc.environment, log)

			assert.Equal(t, tc.serviceName, cfg.ServiceName)
			assert.Equal(t, tc.version, cfg.BuildVersion)
			assert.Equal(t, tc.environment, cfg.Environment)
			assert.Equal(t, "workflow", cfg.Domain)
			assert.Equal(t, observability.Domain, cfg.Domain)
			assert.Same(t, log, cfg.Logger, "the logger is handed on, not wrapped")
		})
	}
}

// A tracing failure is returned with the "tracing: " prefix, there is no
// shutdown to call, and the service metrics are never registered.
func TestGwCInitTelemetry_ATracingFailureReturnsNoShutdownAndTheTracingErrorAndRegistersNoMetrics(t *testing.T) {
	inFreshProcess := gwCFreshProcess(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "none")
	t.Setenv("OTEL_SERVICE_NAME", "")
	log, err := logger.NewLogger("test")
	require.NoError(t, err)

	shutdown, err := observability.InitTelemetry(observability.GinConfig("", "test", "test", log), log)

	require.Error(t, err)
	assert.Nil(t, shutdown, "there is nothing to shut down")
	assert.True(t, strings.HasPrefix(err.Error(), "tracing: "), "got %q", err.Error())
	if inFreshProcess {
		assert.Nil(t, observability.TicketsMintedTotal, "no service collector was created")
		assert.Nil(t, observability.ConnectionsOpen)
		assert.Nil(t, observability.ConnectionsRejectedTotal)
		assert.Nil(t, observability.EventsIngestTotal)
		assert.Nil(t, observability.FramesWrittenTotal)
		assert.Nil(t, observability.InternalAuthRejectedTotal)
	}
}

// Right after Register every closed label set of the ticket mint counter
// already exists at zero, and no other result does.
func TestGwCRegister_TheTicketMintCounterExposesItsThreeResultsAtZero(t *testing.T) {
	inFreshProcess := gwCFreshProcess(t)
	if inFreshProcess {
		require.Nil(t, observability.TicketsMintedTotal, "nothing has registered yet")
	}
	obstest.Init(t)

	series := gwCSeries(t, "workflow_stream_gateway_tickets_minted_total", "result")

	assert.Equal(t, map[string]float64{"ok": 0, "rate_limited": 0, "valkey_unavailable": 0}, series)
}

// Right after Register every closed reason of the rejected-connections counter
// already exists at zero, and no other reason does.
func TestGwCRegister_TheRejectedConnectionsCounterExposesItsThreeReasonsAtZero(t *testing.T) {
	inFreshProcess := gwCFreshProcess(t)
	if inFreshProcess {
		require.Nil(t, observability.ConnectionsRejectedTotal, "nothing has registered yet")
	}
	obstest.Init(t)

	series := gwCSeries(t, "workflow_stream_gateway_connections_rejected_total", "reason")

	assert.Equal(t, map[string]float64{"cap_exceeded": 0, "invalid_ticket": 0, "valkey_unavailable": 0}, series)
}

// A successful init returns a shutdown function and no error, and the six
// service collectors are registered under their documented names and labels.
func TestGwCInitTelemetry_RegistersTheSixServiceCollectorsUnderTheirDocumentedNamesAndLabels(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "none")
	log, err := logger.NewLogger("test")
	require.NoError(t, err)

	shutdown, err := observability.InitTelemetry(observability.GinConfig(observability.ServiceName, "test", "test", log), log)

	require.NoError(t, err)
	require.NotNil(t, shutdown)
	t.Cleanup(shutdown)
	collectors := []struct {
		name      string
		collector prometheus.Collector
		labels    string
	}{
		{"workflow_stream_gateway_tickets_minted_total", observability.TicketsMintedTotal, "result"},
		{"workflow_stream_gateway_connections_open", observability.ConnectionsOpen, ""},
		{"workflow_stream_gateway_connections_rejected_total", observability.ConnectionsRejectedTotal, "reason"},
		{"workflow_stream_gateway_internal_events_ingest_total", observability.EventsIngestTotal, "event_type,result"},
		{"workflow_stream_gateway_frames_written_total", observability.FramesWrittenTotal, "event_type"},
		{"workflow_stream_gateway_internal_auth_rejected_total", observability.InternalAuthRejectedTotal, "reason"},
	}
	for _, c := range collectors {
		t.Run(c.name, func(t *testing.T) {
			require.NotNil(t, c.collector)
			descs := make(chan *prometheus.Desc, 4)
			c.collector.Describe(descs)
			close(descs)
			var all []string
			for d := range descs {
				all = append(all, d.String())
			}
			require.Len(t, all, 1)
			assert.Contains(t, all[0], `fqName: "`+c.name+`"`)
			assert.Contains(t, all[0], "variableLabels: {"+c.labels+"}")

			// Registering the same collector again is refused because it is
			// already on the registry, including the ones with no series yet.
			err := gincommon.MetricsRegisterer().Register(c.collector)
			var already prometheus.AlreadyRegisteredError
			assert.ErrorAs(t, err, &already, "the collector is registered on the shared registry")
		})
	}
}
