package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

// failingResponseWriter wraps a real gin.ResponseWriter and fails every
// Write — used to exercise ginFrameSink's error branches, which a normal
// bytes.Buffer-backed httptest.Recorder practically never hits.
type failingResponseWriter struct {
	gin.ResponseWriter
}

func (w failingResponseWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}

func TestGinFrameSink_WriteErrorsPropagate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	sink := ginFrameSink{w: failingResponseWriter{c.Writer}}

	assert.Error(t, sink.WriteFrame("1-0", "x", []byte(`{}`)))
	assert.Error(t, sink.WriteControlFrame("resync-required", []byte(`{}`)))
	assert.Error(t, sink.WriteHeartbeat())
}

// --- fakes for stream tests ----------------------------------------------

type fakeTicketStore struct {
	tickets  map[string]domain.Ticket
	forceErr error
}

func (f *fakeTicketStore) AllowMint(_ context.Context, _, _ string, _ int, _ time.Duration) (bool, error) {
	return true, nil
}

func (f *fakeTicketStore) PutTicket(_ context.Context, token string, ticket domain.Ticket, _ time.Duration) error {
	f.tickets[token] = ticket
	return nil
}

func (f *fakeTicketStore) GetTicket(_ context.Context, token string) (domain.Ticket, error) {
	if f.forceErr != nil {
		return domain.Ticket{}, f.forceErr
	}
	ticket, ok := f.tickets[token]
	if !ok {
		return domain.Ticket{}, usecase.ErrTicketNotFound
	}
	return ticket, nil
}

// stopAfterOne is a StreamStore whose ReadNew errors immediately, ending
// StreamHandler.Stream's loop deterministically without any real time
// passing or goroutines involved.
type stopAfterOneStreamStore struct {
	oldestExists bool
	rangeFrames  []domain.Frame
	// readNewSeq lets a test exercise one or more live-tail iterations
	// (a heartbeat timeout, then a delivered frame) before the store
	// errors out and ends the loop deterministically.
	readNewSeq   []readNewResult
	readNewIdx   int
	readNewCalls [][]usecase.StreamTarget
}

type readNewResult struct {
	frames []domain.Frame
}

var errHandlerTestStop = errors.New("test: stop stream")

func (s *stopAfterOneStreamStore) OldestEntryID(_ context.Context, _, _ string) (string, bool, error) {
	return "1-0", s.oldestExists, nil
}

func (s *stopAfterOneStreamStore) RangeSince(_ context.Context, _, _, _ string) ([]domain.Frame, error) {
	return s.rangeFrames, nil
}

func (s *stopAfterOneStreamStore) ReadNew(_ context.Context, targets []usecase.StreamTarget, _ time.Duration) ([]domain.Frame, error) {
	s.readNewCalls = append(s.readNewCalls, targets)
	if s.readNewIdx >= len(s.readNewSeq) {
		return nil, errHandlerTestStop
	}
	r := s.readNewSeq[s.readNewIdx]
	s.readNewIdx++
	return r.frames, nil
}

type alwaysAllowLimiter struct {
	allow    bool
	forceErr error
}

func (l *alwaysAllowLimiter) TryAcquire(_ context.Context, _, _ string, _ int, _ time.Duration) (bool, error) {
	if l.forceErr != nil {
		return false, l.forceErr
	}
	return l.allow, nil
}

func (l *alwaysAllowLimiter) Release(_ context.Context, _, _ string) error { return nil }

func newStreamTestRouter(ticketStore *fakeTicketStore, streamStore usecase.StreamStore, limiter usecase.ConnectionLimiter) *gin.Engine {
	gin.SetMode(gin.TestMode)
	tickets := usecase.NewTicketService(ticketStore, time.Minute, 10, time.Minute)
	stream := usecase.NewStreamService(tickets, streamStore, limiter, 5, time.Minute, time.Millisecond)
	r := gin.New()
	r.GET("/api/v1/stream", NewStreamHandler(stream).Stream)
	return r
}

func TestStreamHandler_Stream_MissingTicketReturns401(t *testing.T) {
	r := newStreamTestRouter(&fakeTicketStore{tickets: map[string]domain.Ticket{}}, &stopAfterOneStreamStore{}, &alwaysAllowLimiter{allow: true})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/stream", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestStreamHandler_Stream_TicketStoreUnavailableReturns503(t *testing.T) {
	ticketStore := &fakeTicketStore{tickets: map[string]domain.Ticket{}, forceErr: errors.New("valkey unreachable")}
	r := newStreamTestRouter(ticketStore, &stopAfterOneStreamStore{}, &alwaysAllowLimiter{allow: true})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/stream?ticket=tok", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestStreamHandler_Stream_ConnectionStoreUnavailableReturns503(t *testing.T) {
	ticketStore := &fakeTicketStore{tickets: map[string]domain.Ticket{
		"tok": {TenantID: "tenant-a", UserID: "user-1"},
	}}
	r := newStreamTestRouter(ticketStore, &stopAfterOneStreamStore{}, &alwaysAllowLimiter{forceErr: errors.New("valkey unreachable")})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/stream?ticket=tok", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestStreamHandler_Stream_UnknownTicketReturns401(t *testing.T) {
	r := newStreamTestRouter(&fakeTicketStore{tickets: map[string]domain.Ticket{}}, &stopAfterOneStreamStore{}, &alwaysAllowLimiter{allow: true})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/stream?ticket=unknown", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestStreamHandler_Stream_ConnectionCapExceededReturns429(t *testing.T) {
	ticketStore := &fakeTicketStore{tickets: map[string]domain.Ticket{
		"tok": {TenantID: "tenant-a", UserID: "user-1"},
	}}
	r := newStreamTestRouter(ticketStore, &stopAfterOneStreamStore{}, &alwaysAllowLimiter{allow: false})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/stream?ticket=tok", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusTooManyRequests, rec.Code)
}

func TestStreamHandler_Stream_ValidTicketOpensSSEStream(t *testing.T) {
	ticketStore := &fakeTicketStore{tickets: map[string]domain.Ticket{
		"tok": {TenantID: "tenant-a", UserID: "user-1"},
	}}
	r := newStreamTestRouter(ticketStore, &stopAfterOneStreamStore{}, &alwaysAllowLimiter{allow: true})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/stream?ticket=tok", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "text/event-stream; charset=utf-8", rec.Header().Get("Content-Type"))
	assert.Equal(t, "no-cache, no-transform", rec.Header().Get("Cache-Control"))
}

func TestStreamHandler_Stream_LiveTailWritesHeartbeatAndFrame(t *testing.T) {
	ticketStore := &fakeTicketStore{tickets: map[string]domain.Ticket{
		"tok": {TenantID: "tenant-a", UserID: "user-1"},
	}}
	store := &stopAfterOneStreamStore{
		readNewSeq: []readNewResult{
			{frames: nil}, // timeout -> heartbeat comment
			{frames: []domain.Frame{{ID: "200-0", EventType: "workflow.task.created", Data: []byte(`{"x":1}`)}}},
		},
	}
	r := newStreamTestRouter(ticketStore, store, &alwaysAllowLimiter{allow: true})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/stream?ticket=tok", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, ": keep-alive")
	assert.Contains(t, body, "id: 200-0")
	assert.Contains(t, body, "event: workflow.task.created")
	assert.Contains(t, body, `data: {"x":1}`)
}

func TestStreamHandler_Stream_TenantWideEligibleTicketReadsBroadcastKeyToo(t *testing.T) {
	ticketStore := &fakeTicketStore{tickets: map[string]domain.Ticket{
		"tok": {TenantID: "tenant-a", UserID: "user-1", TenantRoles: []string{"tenant_admin"}},
	}}
	store := &stopAfterOneStreamStore{}
	r := newStreamTestRouter(ticketStore, store, &alwaysAllowLimiter{allow: true})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/stream?ticket=tok", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Len(t, store.readNewCalls, 1)
	assert.ElementsMatch(t, []usecase.StreamTarget{
		{TenantID: "tenant-a", UserID: "user-1"},
		{TenantID: "tenant-a"},
	}, store.readNewCalls[0])
}

func TestStreamHandler_Stream_DepartmentEligibleTicketReadsDepartmentKeyToo(t *testing.T) {
	ticketStore := &fakeTicketStore{tickets: map[string]domain.Ticket{
		"tok": {
			TenantID:    "tenant-a",
			UserID:      "user-1",
			Departments: []domain.DepartmentRole{{DepartmentID: "dept-a", Role: "approver"}},
		},
	}}
	store := &stopAfterOneStreamStore{}
	r := newStreamTestRouter(ticketStore, store, &alwaysAllowLimiter{allow: true})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/stream?ticket=tok", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Len(t, store.readNewCalls, 1)
	assert.ElementsMatch(t, []usecase.StreamTarget{
		{TenantID: "tenant-a", UserID: "user-1"},
		{TenantID: "tenant-a", DepartmentID: "dept-a"},
	}, store.readNewCalls[0])
}

func TestStreamHandler_Stream_BackfillBeyondWindowWritesResyncFrame(t *testing.T) {
	ticketStore := &fakeTicketStore{tickets: map[string]domain.Ticket{
		"tok": {TenantID: "tenant-a", UserID: "user-1"},
	}}
	store := &stopAfterOneStreamStore{oldestExists: false}
	r := newStreamTestRouter(ticketStore, store, &alwaysAllowLimiter{allow: true})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/stream?ticket=tok&last_event_id=1-0", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "event: resync-required")
}

func TestStreamHandler_Stream_LastEventIDHeaderTakesPrecedenceOverQueryParam(t *testing.T) {
	ticketStore := &fakeTicketStore{tickets: map[string]domain.Ticket{
		"tok": {TenantID: "tenant-a", UserID: "user-1"},
	}}
	store := &stopAfterOneStreamStore{oldestExists: false}
	r := newStreamTestRouter(ticketStore, store, &alwaysAllowLimiter{allow: true})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/stream?ticket=tok&last_event_id=1-0", nil)
	req.Header.Set("Last-Event-ID", "2-0")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "event: resync-required")
}
