package http

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

type EventsHandler struct {
	fanout *usecase.FanoutService
}

func NewEventsHandler(fanout *usecase.FanoutService) *EventsHandler {
	return &EventsHandler{fanout: fanout}
}

func (h *EventsHandler) Ingest(c *gin.Context) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		recordEventIngest("unknown", "bad_payload")
		if _, tooLarge := errors.AsType[*http.MaxBytesError](err); tooLarge {
			writeProblem(c, http.StatusRequestEntityTooLarge, "payload-too-large", "request body exceeds the 10 MiB limit")
			return
		}
		writeProblem(c, http.StatusBadRequest, "malformed-envelope", "unable to read request body")
		return
	}

	envelope, err := events.ParseEnvelope[json.RawMessage](body)
	if err != nil {
		recordEventIngest("unknown", "bad_payload")
		writeProblem(c, http.StatusBadRequest, "malformed-envelope", err.Error())
		return
	}

	if !usecase.IsKnownEventType(envelope.Type) {
		recordEventIngest(envelope.Type, "ok")
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
		recordEventIngest(envelope.Type, "bad_payload")
		writeProblem(c, http.StatusBadRequest, "invalid-payload", err.Error())
	case err != nil:
		recordEventIngest(envelope.Type, "error")
		writeProblem(c, http.StatusBadGateway, "ingest-failed", "failed to fan out event")
	default:
		recordEventIngest(envelope.Type, "ok")
		c.Status(http.StatusOK)
	}
}

func recordEventIngest(eventType, result string) {
	if observability.EventsIngestTotal != nil {
		observability.EventsIngestTotal.WithLabelValues(eventType, result).Inc()
	}
}
