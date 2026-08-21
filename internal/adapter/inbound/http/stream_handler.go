package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

// StreamHandler implements GET /api/v1/stream (LLD §5.2). This route is
// deliberately not registered under /api/v1's ProtectedMiddlewares group
// (wired in cmd/server/router.go) — it's carved out of the standard
// gateway JWT chain entirely, since EventSource can't set an Authorization
// header; the ticket (validated here via StreamService) is the sole
// source of connection identity.
type StreamHandler struct {
	stream *usecase.StreamService
}

func NewStreamHandler(stream *usecase.StreamService) *StreamHandler {
	return &StreamHandler{stream: stream}
}

func (h *StreamHandler) Stream(c *gin.Context) {
	token := c.Query("ticket")
	if token == "" {
		writeProblem(c, http.StatusUnauthorized, "unauthorized", "missing ticket query parameter")
		return
	}

	ticket, err := h.stream.ValidateTicket(c.Request.Context(), token)
	switch {
	case errors.Is(err, usecase.ErrTicketNotFound):
		writeProblem(c, http.StatusUnauthorized, "unauthorized", "ticket missing or expired")
		return
	case err != nil:
		writeProblem(c, http.StatusServiceUnavailable, "unavailable", "ticket store unavailable")
		return
	}

	acquired, err := h.stream.AcquireConnection(c.Request.Context(), ticket.TenantID, ticket.UserID)
	if err != nil {
		writeProblem(c, http.StatusServiceUnavailable, "unavailable", "connection store unavailable")
		return
	}
	if !acquired {
		writeProblem(c, http.StatusTooManyRequests, "connection-cap-exceeded", "concurrent connection cap exceeded")
		return
	}
	defer func() {
		// Best-effort: a release that fails to reach Valkey leaks one
		// count until the key's own TTL clears it (LLD §4) — not worth
		// failing the request over on the way out.
		_ = h.stream.ReleaseConnection(context.Background(), ticket.TenantID, ticket.UserID)
	}()

	lastEventID := c.GetHeader("Last-Event-ID")
	if lastEventID == "" {
		lastEventID = c.Query("last_event_id")
	}

	c.Writer.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	c.Writer.Header().Set("Cache-Control", "no-cache, no-transform")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.Header().Set("X-Accel-Buffering", "no")
	c.Writer.WriteHeader(http.StatusOK)

	_ = h.stream.Serve(c.Request.Context(), ticket.TenantID, ticket.UserID, lastEventID, ginFrameSink{w: c.Writer})
}

// ginFrameSink adapts usecase.FrameSink onto gin's ResponseWriter (which
// already satisfies http.Flusher).
type ginFrameSink struct {
	w gin.ResponseWriter
}

func (s ginFrameSink) WriteFrame(id, eventType string, data []byte) error {
	if _, err := fmt.Fprintf(s.w, "id: %s\nevent: %s\ndata: %s\n\n", id, eventType, data); err != nil {
		return fmt.Errorf("write sse frame: %w", err)
	}
	s.w.Flush()
	return nil
}

func (s ginFrameSink) WriteControlFrame(eventType string, data []byte) error {
	if _, err := fmt.Fprintf(s.w, "event: %s\ndata: %s\n\n", eventType, data); err != nil {
		return fmt.Errorf("write sse control frame: %w", err)
	}
	s.w.Flush()
	return nil
}

func (s ginFrameSink) WriteHeartbeat() error {
	if _, err := fmt.Fprint(s.w, ": keep-alive\n\n"); err != nil {
		return fmt.Errorf("write sse heartbeat: %w", err)
	}
	s.w.Flush()
	return nil
}
