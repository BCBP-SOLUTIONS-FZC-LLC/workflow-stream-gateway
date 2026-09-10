// Package http's router.go is the single source of truth for this
// service's HTTP surface: every path, method, middleware, and route group.
// cmd/server's job (cmd/server/wire.go) is to construct dependencies —
// business services, their handlers, the shared gincommon config — and
// call NewRouter, not to encode routing decisions itself.
package http

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/inbound/http/middleware"
)

// shortRequestTimeout bounds POST /api/v1/stream/tickets and POST
// /api/v1/internal/events, the two ordinary request/response endpoints — neither
// has an LLD-documented SLO (docs/lld/workflow_stream_gateway.md is silent
// on this), so this matches execution_service's/definition_service's own
// 30-second convention rather than inventing a number. Deliberately not
// applied to GET /api/v1/stream, which must stay open for the life of an
// SSE connection.
const shortRequestTimeout = 30 * time.Second

// RouterConfig bundles every dependency NewRouter needs: the already-built
// handlers (cmd/server/wire.go's job to construct), the Valkey client
// /readyz pings directly, and the shared gincommon/internal-token
// configuration.
type RouterConfig struct {
	GinConfig        gincommon.Config
	AppEnv           string
	InternalAPIToken string

	ValkeyClient *redis.Client

	TicketHandler *TicketHandler
	EventsHandler *EventsHandler
	StreamHandler *StreamHandler
}

// NewRouter wires the gin.Engine.
//
// ObservabilityMiddlewares is applied at the root, ahead of every route, so
// metrics/logging/tracing cover GET /api/v1/stream and POST
// /api/v1/internal/events too — neither sits under /api/v1's
// ProtectedMiddlewares group, and would otherwise get no gincommon
// middleware at all.
//
// /healthz is a trivial liveness check (process is running, no dependency
// call) and /readyz pings Valkey (readiness — LLD §9.4's fail-closed
// posture belongs on the probe Kubernetes uses to pull a pod out of load-
// balancer rotation, not the one that decides whether to kill the process
// over a dependency blip it didn't cause).
//
// /api/v1 carries LLD §5.1's ticket-mint endpoint behind
// ProtectedMiddlewares. /api/v1/internal is a separate group guarded by
// RequireInternalToken (LLD §3.2) — never nested under the /api/v1 group
// above (it shares the path segment, not the gin group), so it never
// inherits ProtectedMiddlewares or sees a gateway identity header. GET
// /api/v1/stream is registered directly on the root engine, deliberately
// outside every auth middleware group: EventSource can't set an
// Authorization header, and the ticket it carries as a query param is
// validated by the handler itself (LLD §5.2, §9.1).
//
// TimeoutMiddleware is scoped to the /api/v1 and /api/v1/internal groups only
// (ahead of each group's own auth middleware, per TimeoutMiddleware's own
// doc comment), not applied at the root the way execution_service's/
// definition_service's routers do — root-wide here would also cut off GET
// /api/v1/stream. One consequence: ObservabilityMiddlewares, being root-
// level and so outside both groups, always observes whatever status was
// written before Timeout's own post-c.Next() 503-on-expiry runs. That
// matches gincommon's own design elsewhere in this platform — its dedicated
// http_request_timeout_total counter, not http_requests_total or
// HttpRequestDuration, is the intended source of truth for timeout-induced
// 503s (see the library's CHANGELOG entry for that counter).
func NewRouter(cfg RouterConfig) *gin.Engine {
	if cfg.AppEnv != "dev" {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.New()
	for _, mw := range gincommon.ObservabilityMiddlewares(cfg.GinConfig) {
		r.Use(mw)
	}

	r.GET("/healthz", gincommon.HealthHandler())
	r.GET("/readyz", readyzHandler(cfg.ValkeyClient))

	api := r.Group("/api/v1")
	api.Use(gincommon.TimeoutMiddleware(shortRequestTimeout))
	for _, mw := range gincommon.ProtectedMiddlewares(cfg.GinConfig) {
		api.Use(mw)
	}
	api.POST("/stream/tickets", cfg.TicketHandler.Mint)

	// Versioned under /api/v1/internal (not nested under the /api/v1 group
	// above — see the doc comment below) to match execution_service's and
	// definition_service's own convention, so event_consumer's forwarder can
	// target one shared path shape across every service it forwards to.
	internalGroup := r.Group("/api/v1/internal")
	internalGroup.Use(gincommon.TimeoutMiddleware(shortRequestTimeout))
	internalGroup.Use(middleware.RequireInternalToken(cfg.InternalAPIToken))
	internalGroup.POST("/events", cfg.EventsHandler.Ingest)

	r.GET("/api/v1/stream", cfg.StreamHandler.Stream)

	return r
}

// readyzHandler pings Valkey directly rather than through a usecase-level
// port: this service has only the one dependency, so the indirection
// execution_service's Pinger interface exists for (keeping its adapter
// package free of pgcommon/the Temporal SDK) buys nothing here.
func readyzHandler(client *redis.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		if err := client.Ping(c.Request.Context()).Err(); err != nil {
			c.Status(http.StatusServiceUnavailable)
			return
		}
		c.Status(http.StatusOK)
	}
}
