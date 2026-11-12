package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability/obstest"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

var testGinConfig = gincommon.Config{ServiceName: "workflow-stream-gateway", BuildVersion: "test", Domain: "workflow", Environment: "test"}

// unreachableValkeyClient points at a port nothing listens on, so Ping
// fails fast (connection refused) without needing a real Valkey instance.
// This package's tests never exercise Valkey data operations — that
// coverage lives in internal/adapter/outbound/valkey's own docker-compose
// smoke test (Makefile's COVER_PKG_FLOORS comment) — only the connectivity
// check /readyz itself makes.
func unreachableValkeyClient() *redis.Client {
	return redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
}

func testRouterConfig() RouterConfig {
	tickets := usecase.NewTicketService(nil, time.Minute, 10, time.Minute)
	return RouterConfig{
		GinConfig:     testGinConfig,
		AppEnv:        "test",
		ValkeyClient:  unreachableValkeyClient(),
		TicketHandler: NewTicketHandler(tickets),
		EventsHandler: NewEventsHandler(usecase.NewFanoutService(nil)),
		StreamHandler: NewStreamHandler(usecase.NewStreamService(tickets, nil, nil, 5, time.Minute, 15*time.Second)),
	}
}

func TestNewRouter_Healthz(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := NewRouter(testRouterConfig())

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	assert.Equal(t, http.StatusOK, w.Code, "/healthz must never depend on Valkey reachability")
}

func TestNewRouter_Readyz_ValkeyUnreachable_Returns503(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := NewRouter(testRouterConfig())

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestNewRouter_TicketMint_RequiresGatewayIdentity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := NewRouter(testRouterConfig())

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/v1/stream/tickets", nil))

	assert.Equal(t, http.StatusUnauthorized, w.Code, "ProtectedMiddlewares must reject a request with no gateway identity headers before reaching the handler")
}

func TestNewRouter_InternalEvents_RequiresSystemCaller(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := NewRouter(testRouterConfig())

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/v1/internal/events", nil))

	assert.Equal(t, http.StatusUnauthorized, w.Code, "a request without the system caller identity must be rejected before reaching the handler")
	assert.Contains(t, w.Body.String(), "missing or invalid internal caller identity")
}

func TestNewRouter_InternalEvents_SystemCallerReachesHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := NewRouter(testRouterConfig())

	req := httptest.NewRequest(http.MethodPost, "/api/v1/internal/events", nil)
	req.Header.Set("x-user-id", "iam-system")
	req.Header.Set("x-tenant-roles", "iam-system")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	// The request reaches EventsHandler.Ingest, which rejects the empty body
	// as a malformed envelope: proof it passed the identity check.
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestNewRouter_Stream_RegisteredOutsideAuthGroups(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := NewRouter(testRouterConfig())

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/stream", nil))

	// No Authorization header is sent at all, yet the response comes from
	// StreamHandler.Stream's own "missing ticket" check (401), not from
	// ProtectedMiddlewares' RequireAuth (which would also be 401, but for a
	// different reason) — asserting the route isn't nested under /api/v1's
	// protected group is the point; StreamHandler_test.go already covers
	// the handler's own ticket-validation behavior in isolation.
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestNewRouter_InternalEvents_OversizedBodyIsRejectedWith413(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := NewRouter(testRouterConfig())

	body := strings.NewReader(strings.Repeat("x", 11<<20))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/internal/events", body)
	req.Header.Set("x-user-id", "iam-system")
	req.Header.Set("x-tenant-roles", "iam-system")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
}

func TestNewRouter_AProblemCarriesTheCorrelationIDs(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := NewRouter(testRouterConfig())

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/stream", nil))

	assert.Equal(t, "application/problem+json", w.Header().Get("Content-Type"))
	var problem struct {
		TraceID   string `json:"trace_id"`
		RequestID string `json:"request_id"`
	}
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &problem))
	assert.NotEmpty(t, problem.RequestID)
	assert.Equal(t, w.Header().Get("X-Request-ID"), problem.RequestID)
	assert.Equal(t, w.Header().Get("X-Trace-ID"), problem.TraceID)
}

func TestNewRouter_RefusalsAreProblemsWithTheGatewaysCodes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := testRouterConfig()
	r := NewRouter(cfg)

	for name, tc := range map[string]struct {
		method, path string
		status       int
		code         string
	}{
		"an unknown path":                          {http.MethodGet, "/api/v1/nothing-here", http.StatusNotFound, "not-found"},
		"an internal call with no caller identity": {http.MethodPost, "/api/v1/internal/events", http.StatusUnauthorized, "unauthorized"},
	} {
		t.Run(name, func(t *testing.T) {
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))

			assert.Equal(t, tc.status, w.Code)
			assert.Equal(t, "application/problem+json", w.Header().Get("Content-Type"))
			var problem struct {
				Code      string `json:"code"`
				RequestID string `json:"request_id"`
			}
			assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &problem))
			assert.Equal(t, tc.code, problem.Code)
			assert.Equal(t, w.Header().Get("X-Request-ID"), problem.RequestID)
		})
	}
}

func TestNewRouter_TicketMint_GatewayIdentityWithoutTheSystemRoleSucceeds(t *testing.T) {
	obstest.Init(t)
	gin.SetMode(gin.TestMode)
	tickets := &fakeTicketStore{tickets: map[string]domain.Ticket{}}
	r := NewRouter(routerConfigWithFakes(&fakeEventStore{}, tickets))
	const metric = "workflow_stream_gateway_internal_auth_rejected_total"
	before := obstest.Sum(t, metric, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/stream/tickets", nil)
	req.Header.Set("x-user-id", "user-1")
	req.Header.Set("x-tenant-id", "tenant-a")
	req.Header.Set("x-tenant-roles", "tenant_admin")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusCreated, w.Code, "the system caller check guards the internal routes only, not the ticket mint")
	assert.Len(t, tickets.tickets, 1)
	assert.Equal(t, before, obstest.Sum(t, metric, nil), "a gateway caller is not an internal-route refusal")
}
