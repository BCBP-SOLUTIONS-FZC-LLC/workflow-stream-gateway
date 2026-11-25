package middleware

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gwCEngine puts the identity check behind a stand-in for the observability
// middlewares, which set the trace and request ids on the response before any
// handler runs.
func gwCEngine(traceID, requestID string) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		if traceID != "" {
			c.Header("X-Trace-ID", traceID)
		}
		if requestID != "" {
			c.Header("X-Request-ID", requestID)
		}
		c.Next()
	})
	r.Use(RequireSystemCaller())
	r.POST("/api/v1/internal/events", func(c *gin.Context) { c.Status(http.StatusOK) })
	return r
}

// A refused internal call carries the whole problem document, whichever check
// refused it, with the correlation ids taken from the response headers.
func TestGwCRequireSystemCaller_ARefusalWritesTheWholeProblemDocument(t *testing.T) {
	cases := []struct {
		name    string
		headers map[string]string
	}{
		{"no caller identity at all", nil},
		{"a user without the system role", map[string]string{"x-user-id": "00000000-0000-4000-8000-0000000000d1", "x-tenant-roles": "tenant_admin"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/internal/events", nil)
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			rec := httptest.NewRecorder()

			gwCEngine("4bf92f3577b34da6a3ce929d0e0e4736", "req-gwc-1").ServeHTTP(rec, req)

			require.Equal(t, http.StatusUnauthorized, rec.Code)
			assert.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))
			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body), rec.Body.String())
			assert.Equal(t, map[string]any{
				"type":       "https://errors.bcbp.io/workflow-stream-gateway/unauthorized",
				"title":      "Unauthorized",
				"status":     float64(http.StatusUnauthorized),
				"detail":     "missing or invalid internal caller identity",
				"instance":   "/api/v1/internal/events",
				"code":       "unauthorized",
				"trace_id":   "4bf92f3577b34da6a3ce929d0e0e4736",
				"request_id": "req-gwc-1",
			}, body)
		})
	}
}

// Without correlation ids on the response the two fields are left out, not
// written empty.
func TestGwCRequireSystemCaller_ARefusalWithoutCorrelationIDsOmitsThem(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/internal/events", nil)
	rec := httptest.NewRecorder()

	gwCEngine("", "").ServeHTTP(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body), rec.Body.String())
	assert.NotContains(t, body, "trace_id")
	assert.NotContains(t, body, "request_id")
	assert.Equal(t, "unauthorized", body["code"])
}
