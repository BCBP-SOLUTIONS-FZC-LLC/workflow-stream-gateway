package http

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

// EventsHandler implements POST /internal/events (LLD §3.2) — the sole
// ingestion path onto this service, forwarded verbatim by event_consumer.
type EventsHandler struct {
	fanout *usecase.FanoutService
}

func NewEventsHandler(fanout *usecase.FanoutService) *EventsHandler {
	return &EventsHandler{fanout: fanout}
}

func (h *EventsHandler) Ingest(c *gin.Context) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		writeProblem(c, http.StatusBadRequest, "malformed-envelope", "unable to read request body")
		return
	}

	envelope, err := events.ParseEnvelope[json.RawMessage](body)
	if err != nil {
		writeProblem(c, http.StatusBadRequest, "malformed-envelope", err.Error())
		return
	}

	// Forward-compatibility: an event type this service doesn't know about
	// is accepted and ignored with 200, the same convention every other
	// /internal/events handler in this platform uses (LLD §3.2 step 2).
	if !usecase.IsKnownEventType(envelope.Type) {
		c.Status(http.StatusOK)
		return
	}

	err = h.fanout.Ingest(c.Request.Context(), usecase.IngestInput{
		EventType:   envelope.Type,
		TenantID:    envelope.TenantID,
		Payload:     envelope.Payload,
		RawEnvelope: body,
	})

	switch {
	case errors.Is(err, usecase.ErrInvalidPayload):
		// A known type whose payload doesn't match its schema is a
		// producer-side bug, not a delivery problem — non-retryable.
		writeProblem(c, http.StatusBadRequest, "invalid-payload", err.Error())
	case err != nil:
		// Any other failure is a Valkey write failure (LLD §3.2 step 5) —
		// transient and safe to retry; a redelivery that re-writes an
		// already-succeeded recipient's entry is a harmless duplicate.
		writeProblem(c, http.StatusBadGateway, "ingest-failed", "failed to fan out event")
	default:
		c.Status(http.StatusOK)
	}
}
