package http

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/inbound/http/middleware"
)

const shortRequestTimeout = 30 * time.Second

type RouterConfig struct {
	GinConfig        gincommon.Config
	AppEnv           string
	InternalAPIToken string

	ValkeyClient *redis.Client

	TicketHandler *TicketHandler
	EventsHandler *EventsHandler
	StreamHandler *StreamHandler
}

func NewRouter(cfg RouterConfig) *gin.Engine {
	if cfg.AppEnv != "dev" && cfg.AppEnv != "local" {
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
	api.Use(middleware.LimitRequestBody())
	for _, mw := range gincommon.ProtectedMiddlewares(cfg.GinConfig) {
		api.Use(mw)
	}
	api.POST("/stream/tickets", cfg.TicketHandler.Mint)

	internalGroup := r.Group("/api/v1/internal")
	internalGroup.Use(gincommon.TimeoutMiddleware(shortRequestTimeout))
	internalGroup.Use(middleware.LimitRequestBody())
	internalGroup.Use(middleware.RequireInternalToken(cfg.InternalAPIToken))
	internalGroup.POST("/events", cfg.EventsHandler.Ingest)

	r.GET("/api/v1/stream", cfg.StreamHandler.Stream)

	return r
}

func readyzHandler(client *redis.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		if err := client.Ping(c.Request.Context()).Err(); err != nil {
			c.Status(http.StatusServiceUnavailable)
			return
		}
		c.Status(http.StatusOK)
	}
}
