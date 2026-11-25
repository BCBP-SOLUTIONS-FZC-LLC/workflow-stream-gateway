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

func TestRouter_TheRefusedEncodedPathIsNamedAsSent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := NewRouter(routerConfigWithFakes(&fakeEventStore{}, &fakeTicketStore{tickets: map[string]domain.Ticket{}}))
	w := httptest.NewRecorder()

	r.ServeHTTP(w, systemCallerPost("/api/v1/internal%2Fevents", validTaskCreatedEnvelope))

	require.Equal(t, http.StatusNotFound, w.Code)
	var problem problemDetails
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &problem))
	assert.Equal(t, "no route matches POST /api/v1/internal%2Fevents", problem.Detail, "the decoded form names a route that exists")
	assert.Equal(t, "/api/v1/internal%2Fevents", problem.Instance)
}
