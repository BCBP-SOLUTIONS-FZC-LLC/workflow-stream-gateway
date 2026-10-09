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

func newRouter(cfg *config.Config, ginCfg gincommon.Config, client *redis.Client) *gin.Engine {
	ticketStore := sgValkey.NewTicketStore(client)
	ticketService := usecase.NewTicketService(ticketStore, cfg.TicketTTL, cfg.TicketMintRateMax, cfg.TicketMintRateWindow)
	ticketHandler := sgHTTP.NewTicketHandler(ticketService)

	eventStore := sgValkey.NewEventStore(client, cfg.StreamRetentionWindow, cfg.StreamKeyTTL)
	fanoutService := usecase.NewFanoutService(eventStore)
	eventsHandler := sgHTTP.NewEventsHandler(fanoutService)

	streamStore := sgValkey.NewStreamStore(client)
	connLimiter := sgValkey.NewConnectionLimiter(client)
	connCountTTL := cfg.TicketTTL + cfg.HeartbeatInterval
	streamService := usecase.NewStreamService(
		ticketService, streamStore, connLimiter,
		cfg.ConnectionCapPerUser, connCountTTL, cfg.HeartbeatInterval,
	)
	streamHandler := sgHTTP.NewStreamHandler(streamService)

	return sgHTTP.NewRouter(sgHTTP.RouterConfig{
		GinConfig:        ginCfg,
		AppEnv:           cfg.AppEnv,
		InternalAPIToken: cfg.InternalAPIToken,
		ValkeyClient:     client,
		TicketHandler:    ticketHandler,
		EventsHandler:    eventsHandler,
		StreamHandler:    streamHandler,
	})
}
