package main

import (
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	sgHTTP "github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/inbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/inbound/http/middleware"
	sgValkey "github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/outbound/valkey"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

// newRouter wires the gin.Engine: an unprotected /healthz, the
// gateway-authenticated /api/v1 group (LLD §5.1's ticket-mint endpoint),
// and a separate /internal group guarded by RequireInternalToken (LLD
// §3.2) — never nested under /api/v1, so it never inherits
// ProtectedMiddlewares and never sees a gateway identity header. GET
// /api/v1/stream (Phase 4) is deliberately not registered here yet — see
// LLD §5.2/§10.1 on why it can't share this same ProtectedMiddlewares
// group either.
func newRouter(cfg *config.Config, client *redis.Client) *gin.Engine {
	if cfg.AppEnv != "dev" {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.New()
	r.Use(gin.Recovery())

	r.GET("/healthz", func(c *gin.Context) {
		if err := client.Ping(c.Request.Context()).Err(); err != nil {
			c.Status(503)
			return
		}
		c.Status(200)
	})

	ticketStore := sgValkey.NewTicketStore(client)
	ticketService := usecase.NewTicketService(ticketStore, cfg.TicketTTL, cfg.TicketMintRateMax, cfg.TicketMintRateWindow)
	ticketHandler := sgHTTP.NewTicketHandler(ticketService)

	api := r.Group("/api/v1")
	for _, mw := range gincommon.ProtectedMiddlewares(gincommon.Config{ServiceName: "workflow-stream-gateway"}) {
		api.Use(mw)
	}
	api.POST("/stream/tickets", ticketHandler.Mint)

	eventStore := sgValkey.NewEventStore(client, cfg.StreamRetentionWindow, cfg.StreamKeyTTL)
	fanoutService := usecase.NewFanoutService(eventStore)
	eventsHandler := sgHTTP.NewEventsHandler(fanoutService)

	internalGroup := r.Group("/internal")
	internalGroup.Use(middleware.RequireInternalToken(cfg.InternalAPIToken))
	internalGroup.POST("/events", eventsHandler.Ingest)

	return r
}
