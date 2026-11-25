package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRouter_KnownPathWithWrongMethod_Returns404NotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := NewRouter(testRouterConfig())

	cases := []struct {
		name       string
		method     string
		path       string
		wantDetail string
	}{
		{"GET on the ticket mint path", http.MethodGet, "/api/v1/stream/tickets", "no route matches GET /api/v1/stream/tickets"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))

			require.Equal(t, http.StatusNotFound, w.Code)
			assert.Equal(t, "application/problem+json", w.Header().Get("Content-Type"))
			var problem struct {
				Code   string `json:"code"`
				Detail string `json:"detail"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &problem))
			assert.Equal(t, "not-found", problem.Code)
			assert.Equal(t, tc.wantDetail, problem.Detail)
		})
	}
}
