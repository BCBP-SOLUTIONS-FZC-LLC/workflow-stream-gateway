package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

type StreamHandler struct {
	stream   *usecase.StreamService
	stopping context.Context
}

func NewStreamHandler(stream *usecase.StreamService, stopping context.Context) *StreamHandler {
	return &StreamHandler{stream: stream, stopping: stopping}
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
		recordConnectionRejected("invalid_ticket")
		writeProblem(c, http.StatusUnauthorized, "unauthorized", "ticket missing or expired")
		return
	case err != nil:
		recordConnectionRejected("valkey_unavailable")
		writeProblem(c, http.StatusServiceUnavailable, "unavailable", "ticket store unavailable")
		return
	}

	acquired, err := h.stream.AcquireConnection(c.Request.Context(), ticket.TenantID, ticket.UserID)
	if err != nil {
		recordConnectionRejected("valkey_unavailable")
		writeProblem(c, http.StatusServiceUnavailable, "unavailable", "connection store unavailable")
		return
	}
	if !acquired {
		recordConnectionRejected("cap_exceeded")
		writeProblem(c, http.StatusTooManyRequests, "connection-cap-exceeded", "concurrent connection cap exceeded")
		return
	}
	if observability.ConnectionsOpen != nil {
		observability.ConnectionsOpen.Inc()
	}
	defer func() {
		if observability.ConnectionsOpen != nil {
			observability.ConnectionsOpen.Dec()
		}
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
	c.Writer.Flush()

	ctx, cancel := context.WithCancel(c.Request.Context())
	defer cancel()
	stopWatching := context.AfterFunc(h.stopping, cancel)
	defer stopWatching()
	_ = h.stream.Serve(ctx, ticket, lastEventID, ginFrameSink{w: c.Writer})
}

func recordConnectionRejected(reason string) {
	if observability.ConnectionsRejectedTotal != nil {
		observability.ConnectionsRejectedTotal.WithLabelValues(reason).Inc()
	}
}

type ginFrameSink struct {
	w gin.ResponseWriter
}

func (s ginFrameSink) WriteFrame(id, eventType string, data []byte) error {
	if _, err := fmt.Fprintf(s.w, "id: %s\nevent: %s\n%s\n", id, eventType, dataFields(data)); err != nil {
		return fmt.Errorf("write sse frame: %w", err)
	}
	s.w.Flush()
	return nil
}

func (s ginFrameSink) WriteControlFrame(eventType string, data []byte) error {
	if _, err := fmt.Fprintf(s.w, "event: %s\n%s\n", eventType, dataFields(data)); err != nil {
		return fmt.Errorf("write sse control frame: %w", err)
	}
	s.w.Flush()
	return nil
}

var sseLineBreaks = strings.NewReplacer("\r\n", "\n", "\r", "\n")

func dataFields(data []byte) string {
	var b strings.Builder
	for _, line := range strings.Split(sseLineBreaks.Replace(string(data)), "\n") {
		b.WriteString("data: ")
		b.WriteString(line)
		b.WriteString("\n")
	}
	return b.String()
}

func (s ginFrameSink) WriteHeartbeat() error {
	if _, err := fmt.Fprint(s.w, ": keep-alive\n\n"); err != nil {
		return fmt.Errorf("write sse heartbeat: %w", err)
	}
	s.w.Flush()
	return nil
}
