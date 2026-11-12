package http

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

func routerConfigWithFakes(events *fakeEventStore, tickets *fakeTicketStore) RouterConfig {
	cfg := testRouterConfig()
	ticketService := usecase.NewTicketService(tickets, time.Minute, 10, time.Minute)
	cfg.TicketHandler = NewTicketHandler(ticketService)
	cfg.EventsHandler = NewEventsHandler(usecase.NewFanoutService(events))
	cfg.StreamHandler = NewStreamHandler(usecase.NewStreamService(ticketService, nil, nil, 5, time.Minute, 15*time.Second))
	return cfg
}

func systemCallerPost(target, body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(body))
	req.Header.Set("x-user-id", "iam-system")
	req.Header.Set("x-tenant-roles", "iam-system")
	return req
}

func TestRouter_EncodedSlashInTheInternalPrefix_ReachesNoHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	// The mesh policy matches the raw path: /api/v1/internal%2Fevents is a path outside its
	// internal rule, so no caller check may be skipped by routing it as the internal route.
	for _, target := range []string{
		"/api/v1/internal%2Fevents",
		"/api/v1/internal%2fevents",
		"/api/v1%2Finternal/events",
		"/api%2Fv1/internal/events",
	} {
		t.Run(target, func(t *testing.T) {
			events := &fakeEventStore{}
			r := NewRouter(routerConfigWithFakes(events, &fakeTicketStore{tickets: map[string]domain.Ticket{}}))

			w := httptest.NewRecorder()
			r.ServeHTTP(w, systemCallerPost(target, validTaskCreatedEnvelope))

			assert.Equal(t, http.StatusNotFound, w.Code, w.Body.String())
			assert.Zero(t, events.appendCalls+events.broadcastCalls, "no handler may run")
		})
	}
}

func TestRouter_EncodedLetterInTheInternalPrefix_ReachesNoHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	events := &fakeEventStore{}
	r := NewRouter(routerConfigWithFakes(events, &fakeTicketStore{tickets: map[string]domain.Ticket{}}))

	w := httptest.NewRecorder()
	r.ServeHTTP(w, systemCallerPost("/api/v1/%69nternal/events", validTaskCreatedEnvelope))

	assert.Equal(t, http.StatusNotFound, w.Code, "the mesh sees /api/v1/%69nternal, which no policy rule names as internal")
	assert.Zero(t, events.appendCalls+events.broadcastCalls)
}

func TestRouter_PlainRequestsStillReachTheirHandlers(t *testing.T) {
	gin.SetMode(gin.TestMode)
	events := &fakeEventStore{}
	tickets := &fakeTicketStore{tickets: map[string]domain.Ticket{}}
	r := NewRouter(routerConfigWithFakes(events, tickets))

	t.Run("the internal events route", func(t *testing.T) {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, systemCallerPost("/api/v1/internal/events", validTaskCreatedEnvelope))

		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		assert.Equal(t, 2, events.appendCalls)
	})
	t.Run("the ticket mint route", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/stream/tickets", nil)
		req.Header.Set("x-user-id", "user-1")
		req.Header.Set("x-tenant-id", "tenant-a")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)

		require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
		assert.Len(t, tickets.tickets, 1)
	})
	t.Run("the SSE stream route", func(t *testing.T) {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/stream", nil))

		assert.Equal(t, http.StatusUnauthorized, w.Code)
		assert.Contains(t, w.Body.String(), "ticket", "the answer comes from the stream handler's own ticket check")
	})
	t.Run("the SSE stream route with a ticket in the query", func(t *testing.T) {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/stream?ticket=unknown%2Fticket", nil))

		assert.Equal(t, http.StatusUnauthorized, w.Code, "an encoded slash in the query is not a path and must not be refused as one")
	})
}
