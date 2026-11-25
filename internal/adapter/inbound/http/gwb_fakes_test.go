package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/outbound/routingfile"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

const (
	gwBProblemBase    = "https://errors.bcbp.io/workflow-stream-gateway/"
	gwBIngestMetric   = "workflow_stream_gateway_internal_events_ingest_total"
	gwBRejectedMetric = "workflow_stream_gateway_connections_rejected_total"
	gwBOpenMetric     = "workflow_stream_gateway_connections_open"
	gwBMintedMetric   = "workflow_stream_gateway_tickets_minted_total"
	gwBAuthMetric     = "workflow_stream_gateway_internal_auth_rejected_total"

	gwBConsumerCert = `By=spiffe://cluster.local/ns/workflow-app/sa/workflow-stream-gateway;` +
		`Hash=abc;Subject="";URI=spiffe://cluster.local/ns/workflow-app/sa/workflow-events-consumer`

	gwBCreatedType = "workflow.WorkflowTaskCreated"

	gwBValidEnvelope = `{
	"id": "018f2c3e-6b1a-7c40-9e2f-000000000001",
	"type": "WorkflowTaskCreated",
	"source": "workflow-execution-svc",
	"tenant_id": "00000000-0000-4000-8000-0000000000a1",
	"time": "2026-08-21T10:00:00Z",
	"data": {"recipients": [{"kind": "user", "id": "00000000-0000-4000-8000-0000000000b1"}, {"kind": "user", "id": "00000000-0000-4000-8000-0000000000b2"}]}
}`

	gwBInvalidPayloadEnvelope = `{
	"id": "1", "type": "WorkflowTaskCreated", "source": "workflow-execution-svc",
	"time": "2026-08-21T10:00:00Z", "data": {"recipients": [{"kind": "group", "id": "00000000-0000-4000-8000-0000000000f1"}]}
}`
)

var gwBGinConfig = testGinConfig

var errGwBStop = errors.New("gwB: stop stream")

var gwBRoutes = sync.OnceValue(func() *domain.RoutingTable {
	routes, err := routingfile.Load("../../../../config/routing.yaml")
	if err != nil {
		panic(err)
	}
	return routes
})

// gwBEventStore records the appends the fan-out makes and the context each one got.
type gwBEventStore struct {
	users       []string
	broadcasts  int
	departments int
	err         error

	hasDeadline bool
	deadline    time.Time
}

func (s *gwBEventStore) total() int { return len(s.users) + s.broadcasts + s.departments }

func (s *gwBEventStore) AppendEvent(ctx context.Context, _, userID, _ string, _ []byte) error {
	s.deadline, s.hasDeadline = ctx.Deadline()
	if s.err != nil {
		return s.err
	}
	s.users = append(s.users, userID)
	return nil
}

func (s *gwBEventStore) AppendBroadcastEvent(context.Context, string, string, []byte) error {
	if s.err != nil {
		return s.err
	}
	s.broadcasts++
	return nil
}

func (s *gwBEventStore) AppendDepartmentEvent(context.Context, string, string, string, []byte) error {
	if s.err != nil {
		return s.err
	}
	s.departments++
	return nil
}

type gwBPut struct {
	token  string
	ticket domain.Ticket
	ttl    time.Duration
}

type gwBAllow struct {
	tenant, user string
	max          int
	window       time.Duration
}

// gwBTicketStore is a ticket store that records every call it gets.
type gwBTicketStore struct {
	tickets map[string]domain.Ticket
	getErr  error
	deny    bool

	puts   []gwBPut
	allows []gwBAllow
	gets   []string

	allowHasDeadline bool
	allowDeadline    time.Time
	getHasDeadline   bool
	getDeadline      time.Time
}

func newGwBTicketStore() *gwBTicketStore {
	return &gwBTicketStore{tickets: map[string]domain.Ticket{}}
}

func (s *gwBTicketStore) AllowMint(ctx context.Context, tenantID, userID string, max int, window time.Duration) (bool, error) {
	s.allowDeadline, s.allowHasDeadline = ctx.Deadline()
	s.allows = append(s.allows, gwBAllow{tenant: tenantID, user: userID, max: max, window: window})
	return !s.deny, nil
}

func (s *gwBTicketStore) PutTicket(_ context.Context, token string, ticket domain.Ticket, ttl time.Duration) error {
	s.puts = append(s.puts, gwBPut{token: token, ticket: ticket, ttl: ttl})
	s.tickets[token] = ticket
	return nil
}

func (s *gwBTicketStore) GetTicket(ctx context.Context, token string) (domain.Ticket, error) {
	s.getDeadline, s.getHasDeadline = ctx.Deadline()
	s.gets = append(s.gets, token)
	if s.getErr != nil {
		return domain.Ticket{}, s.getErr
	}
	ticket, ok := s.tickets[token]
	if !ok {
		return domain.Ticket{}, usecase.ErrTicketNotFound
	}
	return ticket, nil
}

// gwBStreamStore is a stream store whose live read is a hook.
type gwBStreamStore struct {
	readNew func(ctx context.Context, positions []usecase.StreamPosition) ([]domain.Frame, error)

	latestCalls int
	readCalls   int
}

func (s *gwBStreamStore) KeyFor(t usecase.StreamTarget) string {
	switch {
	case t.UserID != "":
		return "sse:events:" + t.TenantID + ":" + t.UserID
	case t.DepartmentID != "":
		return "sse:events:broadcast:" + t.TenantID + ":" + t.DepartmentID
	default:
		return "sse:events:broadcast:" + t.TenantID
	}
}

func (s *gwBStreamStore) OldestEntryID(context.Context, usecase.StreamTarget) (string, bool, error) {
	return "1-0", true, nil
}

func (s *gwBStreamStore) LatestEntryID(context.Context, usecase.StreamTarget) (string, error) {
	s.latestCalls++
	return "0-0", nil
}

func (s *gwBStreamStore) RangeSince(context.Context, usecase.StreamTarget, string) ([]domain.Frame, error) {
	return nil, nil
}

func (s *gwBStreamStore) ReadNew(ctx context.Context, positions []usecase.StreamPosition, _ time.Duration) ([]domain.Frame, error) {
	s.readCalls++
	if s.readNew == nil {
		return nil, errGwBStop
	}
	return s.readNew(ctx, positions)
}

type gwBAcquire struct {
	tenant, user string
	cap          int
	ttl          time.Duration
}

type gwBRelease struct{ tenant, user string }

// gwBLimiter is a connection limiter that records every call it gets.
type gwBLimiter struct {
	grant      bool
	acquireErr error

	acquires  []gwBAcquire
	releases  []gwBRelease
	refreshes int
}

func (l *gwBLimiter) TryAcquire(_ context.Context, tenantID, userID string, cap int, ttl time.Duration) (bool, error) {
	l.acquires = append(l.acquires, gwBAcquire{tenant: tenantID, user: userID, cap: cap, ttl: ttl})
	if l.acquireErr != nil {
		return false, l.acquireErr
	}
	return l.grant, nil
}

func (l *gwBLimiter) Release(_ context.Context, tenantID, userID string) error {
	l.releases = append(l.releases, gwBRelease{tenant: tenantID, user: userID})
	return nil
}

func (l *gwBLimiter) Refresh(context.Context, string, string, time.Duration) error {
	l.refreshes++
	return nil
}

// gwBWiring holds the fakes behind one router built with the production NewRouter.
type gwBWiring struct {
	tickets  *gwBTicketStore
	events   *gwBEventStore
	stream   *gwBStreamStore
	limiter  *gwBLimiter
	stopping context.Context
}

const (
	gwBConnCap = 5
	gwBConnTTL = 90 * time.Second
)

func newGwBWiring() *gwBWiring {
	return &gwBWiring{
		tickets:  newGwBTicketStore(),
		events:   &gwBEventStore{},
		stream:   &gwBStreamStore{},
		limiter:  &gwBLimiter{grant: true},
		stopping: context.Background(),
	}
}

func gwBTicketFor(tenant, user string) domain.Ticket {
	return domain.Ticket{TenantID: tenant, UserID: user}
}

func (w *gwBWiring) router() *gin.Engine {
	gin.SetMode(gin.TestMode)
	return NewRouter(w.routerConfig())
}

func (w *gwBWiring) routerConfig() RouterConfig {
	tickets := usecase.NewTicketService(w.tickets, 60*time.Second, 10, time.Minute)
	return RouterConfig{
		GinConfig:     gwBGinConfig,
		AppEnv:        "test",
		ValkeyClient:  redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}),
		TicketHandler: NewTicketHandler(tickets),
		EventsHandler: NewEventsHandler(usecase.NewFanoutService(w.events, gwBRoutes(), usecase.CallerCheckOn)),
		StreamHandler: NewStreamHandler(usecase.NewStreamService(tickets, w.stream, w.limiter, gwBConnCap, gwBConnTTL, time.Millisecond), w.stopping),
	}
}

func (w *gwBWiring) serve(r *gin.Engine, req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

func gwBSystemPost(target, body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(body))
	req.Header.Set("x-user-id", "iam-system")
	req.Header.Set("x-tenant-roles", "iam-system")
	req.Header.Set(clientCertHeader, gwBConsumerCert)
	return req
}

type gwBProblem struct {
	Type      string `json:"type"`
	Title     string `json:"title"`
	Status    int    `json:"status"`
	Detail    string `json:"detail"`
	Instance  string `json:"instance"`
	Code      string `json:"code"`
	RequestID string `json:"request_id"`
}

// gwBAssertProblem checks the status line, the media type and the whole problem body.
func gwBAssertProblem(t *testing.T, rec *httptest.ResponseRecorder, status int, code, detail string) gwBProblem {
	t.Helper()
	p := gwBAssertProblemShape(t, rec, status, code)
	assert.Equal(t, detail, p.Detail)
	return p
}

// gwBAssertProblemShape checks everything about a problem except its detail text.
func gwBAssertProblemShape(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) gwBProblem {
	t.Helper()
	require.Equal(t, status, rec.Code, rec.Body.String())
	assert.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))
	var p gwBProblem
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &p), rec.Body.String())
	assert.Equal(t, gwBProblemBase+code, p.Type)
	assert.Equal(t, code, p.Code)
	assert.Equal(t, status, p.Status)
	assert.Equal(t, http.StatusText(status), p.Title)
	assert.NotEmpty(t, p.Instance)
	assert.NotEmpty(t, p.RequestID)
	assert.Equal(t, rec.Header().Get("X-Request-ID"), p.RequestID)
	return p
}

func gwBMintRequest(t *testing.T) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/stream/tickets", nil)
	req.Header.Set("x-tenant-id", testTenantID)
	req.Header.Set("x-user-id", testUserID)
	return req
}
