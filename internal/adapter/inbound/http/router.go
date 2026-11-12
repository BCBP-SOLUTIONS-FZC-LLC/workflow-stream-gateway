package http

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/inbound/http/middleware"
)

const shortRequestTimeout = 30 * time.Second

type RouterConfig struct {
	GinConfig gincommon.Config
	AppEnv    string

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
	r.UseRawPath = true
	r.UnescapePathValues = true
	for _, mw := range gincommon.ObservabilityMiddlewares(cfg.GinConfig) {
		r.Use(mw)
	}
	r.Use(refuseEncodedSlash)

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
	internalGroup.Use(middleware.RequireSystemCaller())
	internalGroup.POST("/events", cfg.EventsHandler.Ingest)

	r.GET("/api/v1/stream", cfg.StreamHandler.Stream)
	r.NoRoute(notFound)

	return r
}

func notFound(c *gin.Context) {
	writeProblem(c, http.StatusNotFound, "not-found", "no route matches "+c.Request.Method+" "+c.Request.URL.Path)
}

func refuseEncodedSlash(c *gin.Context) {
	if strings.Contains(strings.ToLower(c.Request.URL.RawPath), "%2f") {
		notFound(c)
		return
	}
	c.Next()
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
