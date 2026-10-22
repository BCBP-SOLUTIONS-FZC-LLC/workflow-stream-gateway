package observability_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/logger"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/obsregistry"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability"
)

func TestInitTelemetry_RegistersTheSharedAndServiceCollectors(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "none")
	log, err := logger.NewLogger("test")
	require.NoError(t, err)

	shutdown, err := observability.InitTelemetry(observability.GinConfig(observability.ServiceName, "test", "test", log), log)
	require.NoError(t, err)
	t.Cleanup(shutdown)

	gathered, err := gincommon.MetricsGatherer().Gather()
	require.NoError(t, err)
	names := map[string]bool{}
	for _, mf := range gathered {
		names[mf.GetName()] = true
	}
	for _, want := range []string{"platform_build_info", "platform_library_info", "workflow_stream_gateway_connections_rejected_total"} {
		assert.True(t, names[want], want)
	}
	rep, err := obsregistry.Validate(gincommon.MetricsGatherer(), obsregistry.Options{})
	require.NoError(t, err)
	assert.Empty(t, rep.Errors())
}

func TestInitTelemetry_IsRepeatable(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "none")
	log, err := logger.NewLogger("test")
	require.NoError(t, err)
	cfg := observability.GinConfig(observability.ServiceName, "test", "test", log)

	first, err := observability.InitTelemetry(cfg, log)
	require.NoError(t, err)
	t.Cleanup(first)
	second, err := observability.InitTelemetry(cfg, log)
	require.NoError(t, err)
	t.Cleanup(second)
}

func TestInitTelemetry_FailsWithoutAServiceName(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "none")
	t.Setenv("OTEL_SERVICE_NAME", "")
	log, err := logger.NewLogger("test")
	require.NoError(t, err)

	shutdown, err := observability.InitTelemetry(observability.GinConfig("", "test", "test", log), log)
	if err == nil {
		shutdown()
	}
	require.Error(t, err)
}
