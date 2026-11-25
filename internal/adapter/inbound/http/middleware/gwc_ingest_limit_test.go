package middleware_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	gwhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/inbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/inbound/http/middleware"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability/obstest"
)

const gwCTenMiB = 10 << 20

func gwCIngestEngine() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(middleware.LimitRequestBody())
	// The body is refused before the fan-out is reached, so none is needed.
	r.POST("/api/v1/internal/events", gwhttp.NewEventsHandler(nil).Ingest)
	return r
}

// One byte over 10 MiB is a 413 problem with the payload-too-large code and the
// documented detail, and it is counted as a bad payload of unknown type.
func TestGwCIngest_ABodyOverTenMiBIsA413ProblemWithItsCodeAndDetail(t *testing.T) {
	obstest.Init(t)
	const metric = "workflow_stream_gateway_internal_events_ingest_total"
	labels := map[string]string{"event_type": "unknown", "result": "bad_payload"}
	before := obstest.Sum(t, metric, labels)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/internal/events", bytes.NewReader(bytes.Repeat([]byte("x"), gwCTenMiB+1)))
	rec := httptest.NewRecorder()

	gwCIngestEngine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code, rec.Body.String())
	assert.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body), rec.Body.String())
	assert.Equal(t, map[string]any{
		"type":     "https://errors.bcbp.io/workflow-stream-gateway/payload-too-large",
		"title":    "Request Entity Too Large",
		"status":   float64(http.StatusRequestEntityTooLarge),
		"detail":   "request body exceeds the 10 MiB limit",
		"instance": "/api/v1/internal/events",
		"code":     "payload-too-large",
	}, body)
	assert.Equal(t, before+1, obstest.Sum(t, metric, labels))
}

// A body of exactly 10 MiB is read in full, so it fails on its content (a 400
// malformed-envelope problem), not on its size.
func TestGwCIngest_ABodyOfExactlyTenMiBIsReadAndRefusedAsMalformedNotAsTooLarge(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/internal/events", bytes.NewReader(bytes.Repeat([]byte("x"), gwCTenMiB)))
	rec := httptest.NewRecorder()

	gwCIngestEngine().ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	var body struct {
		Code   string `json:"code"`
		Detail string `json:"detail"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body), rec.Body.String())
	assert.Equal(t, "malformed-envelope", body.Code)
	assert.NotEqual(t, "request body exceeds the 10 MiB limit", body.Detail)
}
