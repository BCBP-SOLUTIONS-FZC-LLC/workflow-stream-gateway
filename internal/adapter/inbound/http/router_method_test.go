package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
)

func TestRouter_AnUndeclaredMethodOnAKnownPathIsMethodNotAllowed(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cases := []struct{ method, path, allow string }{
		{http.MethodTrace, "/healthz", "GET"},
		{http.MethodPost, "/readyz", "GET"},
		{http.MethodDelete, "/api/v1/stream", "GET"},
		{http.MethodGet, "/api/v1/stream/tickets", "POST"},
		{http.MethodPut, "/api/v1/internal/events", "POST"},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			r := NewRouter(routerConfigWithFakes(&fakeEventStore{}, &fakeTicketStore{tickets: map[string]domain.Ticket{}}))
			w := httptest.NewRecorder()

			r.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))

			require.Equal(t, http.StatusMethodNotAllowed, w.Code, w.Body.String())
			assert.Equal(t, tc.allow, w.Header().Get("Allow"))
			assert.Contains(t, w.Header().Get("Content-Type"), "application/problem+json")
			var problem problemDetails
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &problem))
			assert.Equal(t, "method-not-allowed", problem.Code)
			assert.Equal(t, tc.path, problem.Instance)
		})
	}
}
