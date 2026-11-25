package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability/obstest"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

const (
	gwATenant = "00000000-0000-4000-8000-00000000a001"
	gwAUser   = "00000000-0000-4000-8000-00000000b001"
	gwAOther  = "00000000-0000-4000-8000-00000000b002"
	gwAKey    = "sse:events:" + gwATenant + ":" + gwAUser

	gwAOpenGauge = "workflow_stream_gateway_connections_open"
)

var errGwABoom = errors.New("gwA: injected failure")

type gwATicketStore struct{ tickets map[string]domain.Ticket }

func (s *gwATicketStore) AllowMint(context.Context, string, string, int, time.Duration) (bool, error) {
	return true, nil
}

func (s *gwATicketStore) PutTicket(context.Context, string, domain.Ticket, time.Duration) error {
	return nil
}

func (s *gwATicketStore) GetTicket(_ context.Context, token string) (domain.Ticket, error) {
	t, ok := s.tickets[token]
	if !ok {
		return domain.Ticket{}, usecase.ErrTicketNotFound
	}
	return t, nil
}

// gwAStore is a hand-rolled StreamStore. onFirstCall runs inside the first store
// call of a connection, which is where the open-connections gauge is sampled.
type gwAStore struct {
	t           *testing.T
	latestErr   error
	oldest      string
	oldestOK    bool
	oldestErr   error
	rangeFrames []domain.Frame
	rangeErr    error
	reads       []gwARead

	gaugeDuring float64
	sampled     bool
	readCalls   int
}

type gwARead struct {
	frames []domain.Frame
	err    error
	run    func()
}

func (s *gwAStore) sample() {
	if !s.sampled {
		s.sampled = true
		s.gaugeDuring = obstest.Sum(s.t, gwAOpenGauge, nil)
	}
}

func (s *gwAStore) KeyFor(t usecase.StreamTarget) string {
	return "sse:events:" + t.TenantID + ":" + t.UserID
}

func (s *gwAStore) LatestEntryID(context.Context, usecase.StreamTarget) (string, error) {
	s.sample()
	if s.latestErr != nil {
		return "", s.latestErr
	}
	if s.oldestOK {
		return "9999999999999-0", nil
	}
	return "0-0", nil
}

func (s *gwAStore) OldestEntryID(context.Context, usecase.StreamTarget) (string, bool, error) {
	s.sample()
	return s.oldest, s.oldestOK, s.oldestErr
}

func (s *gwAStore) RangeSince(_ context.Context, t usecase.StreamTarget, _ string) ([]domain.Frame, error) {
	s.sample()
	if s.rangeErr != nil {
		return nil, s.rangeErr
	}
	out := make([]domain.Frame, len(s.rangeFrames))
	for i, f := range s.rangeFrames {
		f.SourceKey = s.KeyFor(t)
		out[i] = f
	}
	return out, nil
}

func (s *gwAStore) ReadNew(context.Context, []usecase.StreamPosition, time.Duration) ([]domain.Frame, error) {
	s.sample()
	s.readCalls++
	if s.readCalls > len(s.reads) {
		return nil, errGwABoom
	}
	step := s.reads[s.readCalls-1]
	if step.run != nil {
		step.run()
	}
	return step.frames, step.err
}

type gwAReleased struct {
	tenantID, userID string
	ctxErr           error
}

type gwALimiter struct {
	mu       sync.Mutex
	released []gwAReleased
}

func (l *gwALimiter) TryAcquire(context.Context, string, string, int, time.Duration) (bool, error) {
	return true, nil
}

func (l *gwALimiter) Release(ctx context.Context, tenantID, userID string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.released = append(l.released, gwAReleased{tenantID, userID, ctx.Err()})
	return nil
}

func (l *gwALimiter) Refresh(context.Context, string, string, time.Duration) error { return nil }

// gwAFailingRecorder fails every body write, as a client that has gone away does.
type gwAFailingRecorder struct{ *httptest.ResponseRecorder }

func (gwAFailingRecorder) Write([]byte) (int, error) { return 0, errors.New("gwA: connection reset") }

func gwAStreamRouter(store usecase.StreamStore, limiter usecase.ConnectionLimiter) *gin.Engine {
	gin.SetMode(gin.TestMode)
	tickets := usecase.NewTicketService(&gwATicketStore{tickets: map[string]domain.Ticket{
		"tok": {TenantID: gwATenant, UserID: gwAUser},
	}}, time.Minute, 10, time.Minute)
	stream := usecase.NewStreamService(tickets, store, limiter, 5, time.Minute, time.Millisecond)
	r := gin.New()
	r.GET("/api/v1/stream", NewStreamHandler(stream, context.Background()).Stream)
	return r
}

func gwALive(id string) domain.Frame {
	return domain.Frame{ID: id, EventType: "gwa.Live", Data: []byte(`{"n":1}`), SourceKey: gwAKey}
}

// --- P-2-DF-01, P-2-DF-02: a failure after the 200 headers ---------------------

func TestGwAStreamHandler_Stream_AFailureAfterTheHeadersEndsTheStreamQuietly(t *testing.T) {
	obstest.Init(t)
	cases := []struct {
		name        string
		store       func(s *gwAStore)
		target      string
		failWrites  bool
		wantNoBytes bool
	}{
		{name: "live read error", store: func(s *gwAStore) { s.reads = []gwARead{{err: errGwABoom}} }, target: "/api/v1/stream?ticket=tok", wantNoBytes: true},
		{name: "newest entry lookup error", store: func(s *gwAStore) { s.latestErr = errGwABoom }, target: "/api/v1/stream?ticket=tok", wantNoBytes: true},
		{name: "oldest entry lookup error during backfill", store: func(s *gwAStore) { s.oldestErr = errGwABoom }, target: "/api/v1/stream?ticket=tok&last_event_id=" + gwAKey + "=100-0", wantNoBytes: true},
		{
			name: "range error during backfill",
			store: func(s *gwAStore) {
				s.oldest, s.oldestOK, s.rangeErr = "1-0", true, errGwABoom
			},
			target:      "/api/v1/stream?ticket=tok&last_event_id=" + gwAKey + "=100-0",
			wantNoBytes: true,
		},
		{name: "live frame write error", store: func(s *gwAStore) { s.reads = []gwARead{{frames: []domain.Frame{gwALive("200-0")}}} }, target: "/api/v1/stream?ticket=tok", failWrites: true},
		{name: "heartbeat write error", store: func(s *gwAStore) { s.reads = []gwARead{{}} }, target: "/api/v1/stream?ticket=tok", failWrites: true},
		{
			name: "control frame write error",
			store: func(s *gwAStore) {
				s.oldestOK = false
			},
			target:     "/api/v1/stream?ticket=tok&last_event_id=" + gwAKey + "=100-0",
			failWrites: true,
		},
		{
			name: "backfill frame write error",
			store: func(s *gwAStore) {
				s.oldest, s.oldestOK = "1-0", true
				s.rangeFrames = []domain.Frame{{ID: "150-0", EventType: "gwa.Old", Data: []byte(`{}`)}}
			},
			target:     "/api/v1/stream?ticket=tok&last_event_id=" + gwAKey + "=100-0",
			failWrites: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &gwAStore{t: t}
			tc.store(store)
			limiter := &gwALimiter{}
			r := gwAStreamRouter(store, limiter)
			before := obstest.Sum(t, gwAOpenGauge, nil)
			rec := httptest.NewRecorder()
			var w http.ResponseWriter = rec
			if tc.failWrites {
				w = gwAFailingRecorder{rec}
			}

			r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, tc.target, nil))

			result := rec.Result()
			defer func() { _ = result.Body.Close() }()
			assert.Equal(t, http.StatusOK, result.StatusCode, "the 200 headers were already sent, so the failure cannot change the status")
			assert.Equal(t, "text/event-stream; charset=utf-8", result.Header.Get("Content-Type"))
			assert.NotContains(t, rec.Body.String(), "problem", "no problem body is written")
			assert.NotContains(t, rec.Body.String(), `"status"`)
			if tc.wantNoBytes {
				assert.Zero(t, rec.Body.Len(), "nothing at all is written for a store failure")
			}
			assert.Equal(t, before+1, store.gaugeDuring, "the gauge counted the open connection while it was served")
			assert.Equal(t, before, obstest.Sum(t, gwAOpenGauge, nil), "the gauge is decremented when the stream ends")
			require.Len(t, limiter.released, 1, "the connection counter is released once")
			assert.Equal(t, gwAReleased{tenantID: gwATenant, userID: gwAUser}, limiter.released[0], "released for the ticket's tenant and user with a live context")
		})
	}
}

// --- P-2-EC-04: the client disconnects ------------------------------------------

func TestGwAStreamHandler_Stream_AClientDisconnectEndsTheStreamAndFreesTheSlot(t *testing.T) {
	obstest.Init(t)
	cases := []struct {
		name  string
		reads func(cancel context.CancelFunc) []gwARead
	}{
		{
			name: "the live read is cut short by the closing request",
			reads: func(cancel context.CancelFunc) []gwARead {
				return []gwARead{{err: context.Canceled, run: cancel}}
			},
		},
		{
			name: "the client leaves while a frame is delivered",
			reads: func(cancel context.CancelFunc) []gwARead {
				return []gwARead{{frames: []domain.Frame{gwALive("200-0")}, run: cancel}}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			store := &gwAStore{t: t, reads: tc.reads(cancel)}
			limiter := &gwALimiter{}
			r := gwAStreamRouter(store, limiter)
			before := obstest.Sum(t, gwAOpenGauge, nil)
			rec := httptest.NewRecorder()

			r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/stream?ticket=tok", nil).WithContext(ctx))

			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, 1, store.readCalls, "the loop ends on the disconnect, with no further read")
			assert.Equal(t, before+1, store.gaugeDuring)
			assert.Equal(t, before, obstest.Sum(t, gwAOpenGauge, nil), "the gauge is decremented")
			require.Len(t, limiter.released, 1)
			assert.Equal(t, gwAReleased{tenantID: gwATenant, userID: gwAUser}, limiter.released[0],
				"the slot is released with a context that is not the cancelled request's")
		})
	}
}

// --- I-1-HP-01: the accepted event answers 200 with an empty body --------------

type gwAEventStore struct {
	users    []string
	events   []string
	envelope [][]byte
}

func (s *gwAEventStore) AppendEvent(_ context.Context, tenantID, userID, eventType string, raw []byte) error {
	s.users = append(s.users, tenantID+"/"+userID)
	s.events = append(s.events, eventType)
	s.envelope = append(s.envelope, raw)
	return nil
}

func (s *gwAEventStore) AppendBroadcastEvent(_ context.Context, tenantID, eventType string, raw []byte) error {
	s.users = append(s.users, tenantID+"/*")
	s.events = append(s.events, eventType)
	s.envelope = append(s.envelope, raw)
	return nil
}

func (s *gwAEventStore) AppendDepartmentEvent(context.Context, string, string, string, []byte) error {
	return nil
}

func TestGwAEventsHandler_Ingest_AnAcceptedEventAnswers200WithAnEmptyBody(t *testing.T) {
	obstest.Init(t)
	const cert = `By=spiffe://cluster.local/ns/workflow-app/sa/workflow-stream-gateway;Hash=abc;Subject="";` +
		`URI=spiffe://cluster.local/ns/workflow-app/sa/workflow-events-consumer`
	body := `{"id":"018f2c3e-6b1a-7c40-9e2f-000000000001","type":"WorkflowTaskCreated","source":"workflow-execution-svc",` +
		`"tenant_id":"` + gwATenant + `","time":"2026-08-21T10:00:00Z",` +
		`"data":{"recipients":[{"kind":"user","id":"` + gwAUser + `"},{"kind":"user","id":"` + gwAOther + `"}]}}`
	store := &gwAEventStore{}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/internal/events", NewEventsHandler(usecase.NewFanoutService(store, shippedRoutes(t), usecase.CallerCheckOn)).Ingest)
	labels := map[string]string{"event_type": "workflow.WorkflowTaskCreated", "result": "ok"}
	before := obstest.Sum(t, "workflow_stream_gateway_internal_events_ingest_total", labels)
	require.NotNil(t, observability.EventsIngestTotal)

	req := httptest.NewRequest(http.MethodPost, "/internal/events", strings.NewReader(body))
	req.Header.Set("X-Forwarded-Client-Cert", cert)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Zero(t, rec.Body.Len(), "the 200 carries no body")
	assert.Equal(t, []string{gwATenant + "/" + gwAUser, gwATenant + "/" + gwAOther, gwATenant + "/*"}, store.users, "each user, then the tenant administrators")
	assert.Equal(t, []string{"workflow.WorkflowTaskCreated", "workflow.WorkflowTaskCreated", "workflow.WorkflowTaskCreated"}, store.events)
	for _, raw := range store.envelope {
		assert.Equal(t, body, string(raw), "every stream entry carries the envelope as received")
	}
	assert.Equal(t, before+1, obstest.Sum(t, "workflow_stream_gateway_internal_events_ingest_total", labels))
}
