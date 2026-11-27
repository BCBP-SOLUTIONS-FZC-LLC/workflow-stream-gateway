package http

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

const unknownEventType = "unknown"

type EventsHandler struct {
	fanout *usecase.FanoutService
}

func NewEventsHandler(fanout *usecase.FanoutService) *EventsHandler {
	return &EventsHandler{fanout: fanout}
}

func (h *EventsHandler) Ingest(c *gin.Context) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		recordEventIngest(unknownEventType, "bad_payload")
		if _, tooLarge := errors.AsType[*http.MaxBytesError](err); tooLarge {
			writeProblem(c, http.StatusRequestEntityTooLarge, "payload-too-large", "request body exceeds the 10 MiB limit")
			return
		}
		writeProblem(c, http.StatusBadRequest, "malformed-envelope", "unable to read request body")
		return
	}

	envelope, err := events.ParseEnvelope[json.RawMessage](body)
	if err != nil {
		recordEventIngest(unknownEventType, "bad_payload")
		writeProblem(c, http.StatusBadRequest, "malformed-envelope", err.Error())
		return
	}

	if envelope.SchemaID != "" {
		body, err = decodeGlueFramedData(c.Request.Context(), &envelope)
		if err != nil {
			recordEventIngest(unknownEventType, "bad_payload")
			writeProblem(c, http.StatusBadRequest, "invalid-payload", err.Error())
			return
		}
	}

	name, err := h.fanout.Ingest(c.Request.Context(), usecase.IngestInput{
		Source:      envelope.Source,
		Caller:      callerFromClientCert(c.GetHeader(clientCertHeader)),
		EventType:   envelope.Type,
		TenantID:    envelope.TenantID,
		Payload:     envelope.Payload,
		RawEnvelope: body,
	})

	switch {
	case errors.Is(err, usecase.ErrUnknownEventType):
		recordEventIngest(unknownEventType, "ok")
		c.Status(http.StatusOK)
	case errors.Is(err, usecase.ErrCallerNotAllowed):
		recordCallerRejection()
		writeProblem(c, http.StatusForbidden, "forbidden", "the caller may not publish events for this source")
	case errors.Is(err, usecase.ErrInvalidPayload):
		recordEventIngest(name, "bad_payload")
		writeProblem(c, http.StatusBadRequest, "invalid-payload", err.Error())
	case err != nil:
		recordEventIngest(name, "error")
		writeProblem(c, http.StatusBadGateway, "ingest-failed", "failed to fan out event")
	default:
		recordEventIngest(name, "ok")
		c.Status(http.StatusOK)
	}
}

func decodeGlueFramedData(ctx context.Context, envelope *events.Envelope[json.RawMessage]) ([]byte, error) {
	var encoded string
	if err := json.Unmarshal(envelope.Payload, &encoded); err != nil {
		return nil, fmt.Errorf("data of a schema-registry event must be a base64 string: %w", err)
	}
	framed, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("data of a schema-registry event is not valid base64: %w", err)
	}
	payload, err := events.GlueDecodeCodec{}.Decode(ctx, envelope.SchemaID, framed)
	if err != nil {
		return nil, fmt.Errorf("data of a schema-registry event: %w", err)
	}
	envelope.Payload = payload
	envelope.SchemaID = ""
	rewritten, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("re-encode the decoded envelope: %w", err)
	}
	return rewritten, nil
}

func recordCallerRejection() {
	if observability.InternalAuthRejectedTotal != nil {
		observability.InternalAuthRejectedTotal.WithLabelValues("caller_not_allowed").Inc()
	}
}

func recordEventIngest(eventType, result string) {
	if observability.EventsIngestTotal != nil {
		observability.EventsIngestTotal.WithLabelValues(eventType, result).Inc()
	}
}
