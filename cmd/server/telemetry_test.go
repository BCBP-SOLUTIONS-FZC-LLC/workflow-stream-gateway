package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/logger"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/obsregistry"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability"
)

var ruleMetricPattern = regexp.MustCompile(`workflow_stream_gateway_[a-z_]+`)

func TestStandard_LibrariesShareRegistryCollectors(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "none")
	log, err := logger.NewLogger("test")
	require.NoError(t, err)

	ginCfg := observability.GinConfig(observability.ServiceName, "test", "test", log)
	shutdown, err := observability.InitTelemetry(ginCfg, log)
	require.NoError(t, err)
	t.Cleanup(shutdown)

	gin.SetMode(gin.TestMode)
	engine := gin.New()
	for _, mw := range gincommon.ObservabilityMiddlewares(ginCfg) {
		engine.Use(mw)
	}
	engine.GET("/healthz", gincommon.HealthHandler())
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	observability.TicketsMintedTotal.WithLabelValues("ok").Inc()
	observability.ConnectionsOpen.Inc()
	observability.ConnectionsRejectedTotal.WithLabelValues("cap_exceeded").Inc()
	observability.EventsIngestTotal.WithLabelValues("WorkflowTaskCreated", "ok").Inc()
	observability.FramesWrittenTotal.WithLabelValues("WorkflowTaskCreated").Inc()

	gathered, err := gincommon.MetricsGatherer().Gather()
	require.NoError(t, err)
	names := map[string]bool{}
	for _, mf := range gathered {
		names[mf.GetName()] = true
	}
	for _, want := range []string{
		"platform_build_info",
		"platform_library_info",
		"platform_http_requests_total",
		"platform_http_request_duration_seconds",
		"workflow_stream_gateway_tickets_minted_total",
		"workflow_stream_gateway_connections_open",
		"workflow_stream_gateway_connections_rejected_total",
		"workflow_stream_gateway_internal_events_ingest_total",
		"workflow_stream_gateway_frames_written_total",
	} {
		assert.True(t, names[want], "%s must be exported", want)
	}

	for _, rules := range []string{"../../deploy/helm/templates/prometheusrule.yaml"} {
		body, readErr := os.ReadFile(rules)
		if readErr != nil {
			continue
		}
		for _, line := range strings.Split(string(body), "\n") {
			if strings.Contains(line, "name: workflow_stream_gateway_") {
				continue
			}
			for _, token := range ruleMetricPattern.FindAllString(line, -1) {
				name := strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(token, "_bucket"), "_count"), "_sum")
				assert.True(t, names[name], "%s references %s, which the service does not export", rules, token)
			}
		}
	}

	rep, err := obsregistry.Validate(gincommon.MetricsGatherer(), obsregistry.Options{})
	require.NoError(t, err)
	assert.Empty(t, rep.Errors(), "scrape must conform to the Platform Observability Registry")

	if out := os.Getenv("METRICS_SCRAPE_OUT"); out != "" {
		scrape := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil)
		req.Header.Set("Accept", "text/plain")
		gincommon.MetricsHandler().ServeHTTP(scrape, req)
		require.NoError(t, os.WriteFile(out, scrape.Body.Bytes(), 0o600))
	}
}
