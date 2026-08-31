package main

import (
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	sgHTTP "github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/inbound/http"
	sgValkey "github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/outbound/valkey"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

// serviceName is the gincommon Prometheus const label / OTEL service.name —
// this service has no OTELServiceName config knob (unlike execution_service's
// cfg.OTELServiceName), so it's a literal here, matching the pre-refactor
// inline literal in the router-wiring this file replaces.
const serviceName = "workflow-stream-gateway"

// newRouter is this service's composition root: it builds every business-
// object graph (ticket/fanout/stream services, their handlers) and the
// shared gincommon config, then hands them to sgHTTP.NewRouter. main.go's
// job is process lifecycle only (listen, signal handling, shutdown), not
// construction — mirrors execution_service's/definition_service's own
// cmd/server composition-root split.
func newRouter(cfg *config.Config, client *redis.Client, log Logger) *gin.Engine {
	ticketStore := sgValkey.NewTicketStore(client)
	ticketService := usecase.NewTicketService(ticketStore, cfg.TicketTTL, cfg.TicketMintRateMax, cfg.TicketMintRateWindow)
	ticketHandler := sgHTTP.NewTicketHandler(ticketService)

	eventStore := sgValkey.NewEventStore(client, cfg.StreamRetentionWindow, cfg.StreamKeyTTL)
	fanoutService := usecase.NewFanoutService(eventStore)
	eventsHandler := sgHTTP.NewEventsHandler(fanoutService)

	streamStore := sgValkey.NewStreamStore(client)
	connLimiter := sgValkey.NewConnectionLimiter(client)
	// LLD §4 requires the conncount key's own TTL to exceed the ticket
	// TTL, without naming an exact value — TicketTTL+HeartbeatInterval is
	// simply a value guaranteed to satisfy that, not its own config knob.
	connCountTTL := cfg.TicketTTL + cfg.HeartbeatInterval
	streamService := usecase.NewStreamService(
		ticketService, streamStore, connLimiter,
		cfg.ConnectionCapPerUser, connCountTTL, cfg.HeartbeatInterval,
	)
	streamHandler := sgHTTP.NewStreamHandler(streamService)

	return sgHTTP.NewRouter(sgHTTP.RouterConfig{
		GinConfig: gincommon.Config{
			Logger:       log,
			ServiceName:  serviceName,
			BuildVersion: cfg.BuildVersion,
		},
		AppEnv:           cfg.AppEnv,
		InternalAPIToken: cfg.InternalAPIToken,
		ValkeyClient:     client,
		TicketHandler:    ticketHandler,
		EventsHandler:    eventsHandler,
		StreamHandler:    streamHandler,
	})
}
