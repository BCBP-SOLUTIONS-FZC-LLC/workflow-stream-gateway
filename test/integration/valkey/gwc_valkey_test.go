//go:build integration

package valkey_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
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

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	gwhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/inbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/outbound/valkey"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability/obstest"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/test/fixtures"
)

const (
	gwCTenant      = "00000000-0000-4000-8000-0000000000c1"
	gwCOtherTenant = "00000000-0000-4000-8000-0000000000c2"
	gwCUser        = "00000000-0000-4000-8000-0000000000d1"
	gwCOtherUser   = "00000000-0000-4000-8000-0000000000d2"
	gwCDeptA       = "00000000-0000-4000-8000-0000000000e1"
	gwCDeptB       = "00000000-0000-4000-8000-0000000000e2"
)

func gwCClient(t *testing.T) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: fixtures.NewTestValkey(t)})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// gwCSibling is a second client on the same server, so a test can fail commands
// on one connection while it checks state through another.
func gwCSibling(t *testing.T, client *redis.Client, name string) *redis.Client {
	t.Helper()
	sibling := redis.NewClient(&redis.Options{Addr: client.Options().Addr, ClientName: name})
	t.Cleanup(func() { _ = sibling.Close() })
	return sibling
}

// gwCHook records every command name a client sends and fails the ones a test
// has told it to, the way a Valkey hiccup would.
type gwCHook struct {
	mu      sync.Mutex
	failing map[string]bool
	seen    []string
}

func (h *gwCHook) fail(names ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.failing = map[string]bool{}
	for _, n := range names {
		h.failing[n] = true
	}
}

func (h *gwCHook) count(name string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, s := range h.seen {
		if s == name {
			n++
		}
	}
	return n
}

func (h *gwCHook) seenNames() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.seen...)
}

func (h *gwCHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) { return next(ctx, network, addr) }
}

func (h *gwCHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		name := strings.ToLower(cmd.Name())
		h.mu.Lock()
		h.seen = append(h.seen, name)
		failing := h.failing[name]
		h.mu.Unlock()
		if failing {
			err := errors.New("injected " + name + " failure")
			cmd.SetErr(err)
			return err
		}
		return next(ctx, cmd)
	}
}

func (h *gwCHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func gwCBlockedInXRead(client *redis.Client, name string) bool {
	clients, err := client.ClientList(context.Background()).Result()
	if err != nil {
		return false
	}
	for _, line := range strings.Split(clients, "\n") {
		if strings.Contains(line, "name="+name) && strings.Contains(line, "cmd=xread") && strings.Contains(line, "flags=b") {
			return true
		}
	}
	return false
}

func gwCEntryIDs(t *testing.T, client *redis.Client, key string) []string {
	t.Helper()
	entries, err := client.XRange(context.Background(), key, "-", "+").Result()
	require.NoError(t, err)
	ids := make([]string, len(entries))
	for i, e := range entries {
		ids[i] = e.ID
	}
	return ids
}

type gwCSink struct {
	types   []string
	onFrame func()
}

func (s *gwCSink) WriteFrame(_, eventType string, _ []byte) error {
	s.types = append(s.types, eventType)
	if s.onFrame != nil {
		s.onFrame()
	}
	return nil
}
func (s *gwCSink) WriteControlFrame(string, []byte) error { return nil }
func (s *gwCSink) WriteHeartbeat() error                  { return nil }

// gwCServeOneEvent opens a stream for the ticket, waits until it is blocked in
// its first live read, appends one event and returns once that event has been
// written to the sink and the stream has ended.
func gwCServeOneEvent(t *testing.T, svc *usecase.StreamService, client *redis.Client, readerName string, ticket domain.Ticket, eventType string) []string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := &gwCSink{onFrame: cancel}
	served := make(chan error, 1)
	go func() { served <- svc.Serve(ctx, ticket, "", sink) }()

	require.Eventually(t, func() bool { return gwCBlockedInXRead(client, readerName) }, 10*time.Second, 5*time.Millisecond, "the stream waits in its first live read")
	events := valkey.NewEventStore(client, time.Hour, time.Hour)
	require.NoError(t, events.AppendEvent(context.Background(), ticket.TenantID, ticket.UserID, eventType, []byte(`{}`)))
	require.NoError(t, <-served)
	return sink.types
}

// The grant sets the counter's expiry to the given ttl; a refusal at the cap
// changes neither the count nor the expiry. Counters are per tenant and user.
func TestGwCConnectionLimiter_AGrantSetsTheCounterAndItsTTLAndARefusalLeavesBoth(t *testing.T) {
	client := gwCClient(t)
	limiter := valkey.NewConnectionLimiter(client)
	ctx := context.Background()
	key := "sse:conncount:" + gwCTenant + ":" + gwCUser
	const ttl = time.Hour

	ok, err := limiter.TryAcquire(ctx, gwCTenant, gwCUser, 2, ttl)
	require.NoError(t, err)
	assert.True(t, ok)
	count, err := client.Get(ctx, key).Result()
	require.NoError(t, err)
	assert.Equal(t, "1", count)
	pttl, err := client.PTTL(ctx, key).Result()
	require.NoError(t, err)
	assert.Greater(t, pttl, ttl/2, "the grant gave the counter the requested expiry")
	assert.LessOrEqual(t, pttl, ttl)

	require.NoError(t, client.PExpire(ctx, key, 30*time.Minute).Err())
	ok, err = limiter.TryAcquire(ctx, gwCTenant, gwCUser, 2, ttl)
	require.NoError(t, err)
	assert.True(t, ok)
	count, err = client.Get(ctx, key).Result()
	require.NoError(t, err)
	assert.Equal(t, "2", count)
	pttl, err = client.PTTL(ctx, key).Result()
	require.NoError(t, err)
	assert.Greater(t, pttl, 30*time.Minute, "a grant sets the expiry again")

	require.NoError(t, client.PExpire(ctx, key, 20*time.Minute).Err())
	ok, err = limiter.TryAcquire(ctx, gwCTenant, gwCUser, 2, ttl)
	require.NoError(t, err)
	assert.False(t, ok, "the cap is reached")
	count, err = client.Get(ctx, key).Result()
	require.NoError(t, err)
	assert.Equal(t, "2", count, "a refusal does not count")
	pttl, err = client.PTTL(ctx, key).Result()
	require.NoError(t, err)
	assert.Greater(t, pttl, time.Duration(0))
	assert.LessOrEqual(t, pttl, 20*time.Minute, "a refusal does not refresh the expiry")

	for name, who := range map[string][2]string{
		"another user of the same tenant": {gwCTenant, gwCOtherUser},
		"the same user in another tenant": {gwCOtherTenant, gwCUser},
		"another user in another tenant":  {gwCOtherTenant, gwCOtherUser},
	} {
		ok, err = limiter.TryAcquire(ctx, who[0], who[1], 2, ttl)
		require.NoError(t, err, name)
		assert.True(t, ok, "%s has a counter of its own", name)
		otherKey := "sse:conncount:" + who[0] + ":" + who[1]
		count, err = client.Get(ctx, otherKey).Result()
		require.NoError(t, err, name)
		assert.Equal(t, "1", count, name)
		pttl, err = client.PTTL(ctx, otherKey).Result()
		require.NoError(t, err, name)
		assert.Greater(t, pttl, time.Duration(0), name)
	}
	count, err = client.Get(ctx, key).Result()
	require.NoError(t, err)
	assert.Equal(t, "2", count, "the first user's counter is not moved by the others")
}

// A script that fails (here on a key holding the wrong type) is an error, never
// a grant, and it leaves the key exactly as it found it, with no expiry added.
func TestGwCConnectionLimiter_AFailingScriptIsAWrappedErrorNotAGrantAndChangesNothing(t *testing.T) {
	client := gwCClient(t)
	limiter := valkey.NewConnectionLimiter(client)
	ctx := context.Background()
	key := "sse:conncount:" + gwCTenant + ":" + gwCUser
	require.NoError(t, client.RPush(ctx, key, "planted").Err())

	granted, err := limiter.TryAcquire(ctx, gwCTenant, gwCUser, 5, time.Hour)
	assert.False(t, granted, "an error is never a grant")
	require.Error(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "valkey acquire: "), "got %q", err.Error())
	assert.Contains(t, err.Error(), "WRONGTYPE")

	err = limiter.Release(ctx, gwCTenant, gwCUser)
	require.Error(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "valkey release: "), "got %q", err.Error())
	assert.Contains(t, err.Error(), "WRONGTYPE")

	values, err := client.LRange(ctx, key, 0, -1).Result()
	require.NoError(t, err)
	assert.Equal(t, []string{"planted"}, values, "neither script changed the key")
	pttl, err := client.PTTL(ctx, key).Result()
	require.NoError(t, err)
	assert.Equal(t, time.Duration(-1), pttl, "the failed acquire did not set an expiry")
}

// The mint-rate script fails as one call: the caller gets an error, never an
// allowance, and the counter is not given an expiry.
func TestGwCTicketStore_AFailingMintRateScriptIsAWrappedErrorNotAnAllowance(t *testing.T) {
	client := gwCClient(t)
	store := valkey.NewTicketStore(client)
	ctx := context.Background()
	key := "sse:ticketrate:" + gwCTenant + ":" + gwCUser
	require.NoError(t, client.RPush(ctx, key, "planted").Err())

	allowed, err := store.AllowMint(ctx, gwCTenant, gwCUser, 1000, time.Minute)

	assert.False(t, allowed, "an error is never an allowance")
	require.Error(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "valkey mint rate: "), "got %q", err.Error())
	assert.Contains(t, err.Error(), "WRONGTYPE")
	values, err := client.LRange(ctx, key, 0, -1).Result()
	require.NoError(t, err)
	assert.Equal(t, []string{"planted"}, values)
	pttl, err := client.PTTL(ctx, key).Result()
	require.NoError(t, err)
	assert.Equal(t, time.Duration(-1), pttl, "no expiry was set by the failed call")
}

// A missing ticket, an undecodable one and a store failure are three different
// answers, and only the first is ErrTicketNotFound.
func TestGwCTicketStore_GetTicketTellsAMissingACorruptAndAFailedReadApart(t *testing.T) {
	client := gwCClient(t)
	store := valkey.NewTicketStore(client)
	ctx := context.Background()
	require.NoError(t, client.Set(ctx, "ssegw:ticket:syntax", "not json", time.Minute).Err())
	require.NoError(t, client.Set(ctx, "ssegw:ticket:shape", `{"tenant_id":5}`, time.Minute).Err())
	require.NoError(t, client.RPush(ctx, "ssegw:ticket:wrongtype", "planted").Err())

	t.Run("missing", func(t *testing.T) {
		ticket, err := store.GetTicket(ctx, "missing")

		assert.ErrorIs(t, err, usecase.ErrTicketNotFound)
		assert.Equal(t, domain.Ticket{}, ticket)
	})
	t.Run("not JSON", func(t *testing.T) {
		ticket, err := store.GetTicket(ctx, "syntax")

		require.Error(t, err)
		assert.NotErrorIs(t, err, usecase.ErrTicketNotFound)
		assert.True(t, strings.HasPrefix(err.Error(), "unmarshal ticket: "), "got %q", err.Error())
		var syntax *json.SyntaxError
		assert.ErrorAs(t, err, &syntax)
		assert.Equal(t, domain.Ticket{}, ticket)
	})
	t.Run("JSON of the wrong shape", func(t *testing.T) {
		ticket, err := store.GetTicket(ctx, "shape")

		require.Error(t, err)
		assert.NotErrorIs(t, err, usecase.ErrTicketNotFound)
		assert.True(t, strings.HasPrefix(err.Error(), "unmarshal ticket: "), "got %q", err.Error())
		var shape *json.UnmarshalTypeError
		assert.ErrorAs(t, err, &shape)
		assert.Equal(t, domain.Ticket{}, ticket)
	})
	t.Run("the read itself fails", func(t *testing.T) {
		ticket, err := store.GetTicket(ctx, "wrongtype")

		require.Error(t, err)
		assert.NotErrorIs(t, err, usecase.ErrTicketNotFound)
		assert.True(t, strings.HasPrefix(err.Error(), "valkey get: "), "got %q", err.Error())
		assert.Contains(t, err.Error(), "WRONGTYPE")
		assert.Equal(t, domain.Ticket{}, ticket)
	})
}

// When the ticket SET fails, nothing is stored, the mint is an error rather than
// a token, and the HTTP answer is the 503 "unavailable" problem.
func TestGwCTicketStore_AFailingSetStoresNothingAndTheMintAnswers503(t *testing.T) {
	obstest.Init(t)
	client := gwCClient(t)
	failing := gwCSibling(t, client, "gwc-set-fails")
	hook := &gwCHook{}
	failing.AddHook(hook)
	hook.fail("set")
	store := valkey.NewTicketStore(failing)
	ctx := context.Background()

	err := store.PutTicket(ctx, "tok", domain.Ticket{TenantID: gwCTenant, UserID: gwCUser}, time.Minute)
	require.Error(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "valkey set: "), "got %q", err.Error())
	assert.Contains(t, err.Error(), "injected set failure")

	service := usecase.NewTicketService(store, time.Minute, 10, time.Minute)
	token, ttl, err := service.Mint(ctx, domain.Ticket{TenantID: gwCTenant, UserID: gwCUser})
	require.Error(t, err)
	assert.NotErrorIs(t, err, usecase.ErrRateLimited)
	assert.True(t, strings.HasPrefix(err.Error(), "ticket mint store: valkey set: "), "got %q", err.Error())
	assert.Empty(t, token)
	assert.Zero(t, ttl)

	gin.SetMode(gin.TestMode)
	router := gwhttp.NewRouter(gwhttp.RouterConfig{
		GinConfig:     gincommon.Config{ServiceName: "workflow-stream-gateway", BuildVersion: "test", Domain: "workflow", Environment: "test"},
		AppEnv:        "test",
		ValkeyClient:  client,
		TicketHandler: gwhttp.NewTicketHandler(service),
	})
	const metric = "workflow_stream_gateway_tickets_minted_total"
	labels := map[string]string{"result": "valkey_unavailable"}
	before := obstest.Sum(t, metric, labels)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/stream/tickets", nil)
	req.Header.Set("x-tenant-id", gwCTenant)
	req.Header.Set("x-user-id", gwCUser)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	assert.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))
	var problem struct {
		Type, Title, Detail, Instance, Code string
		Status                              int
		RequestID                           string `json:"request_id"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &problem), rec.Body.String())
	assert.Equal(t, "https://errors.bcbp.io/workflow-stream-gateway/unavailable", problem.Type)
	assert.Equal(t, "Service Unavailable", problem.Title)
	assert.Equal(t, http.StatusServiceUnavailable, problem.Status)
	assert.Equal(t, "ticket store unavailable", problem.Detail)
	assert.Equal(t, "/api/v1/stream/tickets", problem.Instance)
	assert.Equal(t, "unavailable", problem.Code)
	assert.NotEmpty(t, problem.RequestID)
	assert.NotContains(t, rec.Body.String(), "token", "no ticket is returned")
	assert.Equal(t, before+1, obstest.Sum(t, metric, labels))

	stored, err := client.Keys(ctx, "ssegw:ticket:*").Result()
	require.NoError(t, err)
	assert.Empty(t, stored, "no ticket reached Valkey")
}

// The event store trims entries older than the retention window on each append
// (MINID) and keeps the key alive for its own TTL.
func TestGwCEventStore_AnAppendTrimsEntriesOlderThanTheRetentionWindow(t *testing.T) {
	client := gwCClient(t)
	ctx := context.Background()
	// Approximate trimming removes whole nodes only; one entry per node makes
	// the trim exact enough to observe on a three-entry stream.
	require.NoError(t, client.ConfigSet(ctx, "stream-node-max-entries", "1").Err())
	key := "sse:events:" + gwCTenant + ":" + gwCUser
	recent := fmt.Sprintf("%d-0", time.Now().Add(-time.Minute).UnixMilli())
	for _, id := range []string{"1000-0", "2000-0", recent} {
		require.NoError(t, client.XAdd(ctx, &redis.XAddArgs{Stream: key, ID: id, Values: map[string]any{"event": "seed", "data": id}}).Err())
	}
	require.Equal(t, []string{"1000-0", "2000-0", recent}, gwCEntryIDs(t, client, key))

	wide := valkey.NewEventStore(client, 10*time.Minute, 15*time.Minute)
	require.NoError(t, wide.AppendEvent(ctx, gwCTenant, gwCUser, "gwc.First", []byte(`{"n":1}`)))

	ids := gwCEntryIDs(t, client, key)
	require.Len(t, ids, 2, "the two entries older than ten minutes are gone, the one a minute old stays")
	assert.Equal(t, recent, ids[0])
	entries, err := client.XRange(ctx, key, ids[1], ids[1]).Result()
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "gwc.First", entries[0].Values["event"])
	assert.Equal(t, `{"n":1}`, entries[0].Values["data"])
	ttl, err := client.TTL(ctx, key).Result()
	require.NoError(t, err)
	assert.Greater(t, ttl, 10*time.Minute, "the key outlives the retention window")
	assert.LessOrEqual(t, ttl, 15*time.Minute)

	narrow := valkey.NewEventStore(client, 30*time.Second, time.Hour)
	require.NoError(t, narrow.AppendEvent(ctx, gwCTenant, gwCUser, "gwc.Second", []byte(`{"n":2}`)))

	after := gwCEntryIDs(t, client, key)
	require.Len(t, after, 2, "a 30 second window drops the entry from a minute ago")
	assert.Equal(t, ids[1], after[0])
	ttl, err = client.TTL(ctx, key).Result()
	require.NoError(t, err)
	assert.Greater(t, ttl, 15*time.Minute, "every append sets the key's TTL again")
}

// Personal, tenant-wide and department events go to distinct keys, and a read
// of one target returns only its own events.
func TestGwCEventStore_EachScopeWritesItsOwnKeyAndNoReadSeesAnotherScope(t *testing.T) {
	client := gwCClient(t)
	events := valkey.NewEventStore(client, 10*time.Minute, 15*time.Minute)
	streams := valkey.NewStreamStore(client)
	ctx := context.Background()

	require.NoError(t, events.AppendEvent(ctx, gwCTenant, gwCUser, "gwc.User", []byte(`{}`)))
	require.NoError(t, events.AppendEvent(ctx, gwCTenant, gwCOtherUser, "gwc.OtherUser", []byte(`{}`)))
	require.NoError(t, events.AppendBroadcastEvent(ctx, gwCTenant, "gwc.Tenant", []byte(`{}`)))
	require.NoError(t, events.AppendDepartmentEvent(ctx, gwCTenant, gwCDeptA, "gwc.DeptA", []byte(`{}`)))
	require.NoError(t, events.AppendDepartmentEvent(ctx, gwCTenant, gwCDeptB, "gwc.DeptB", []byte(`{}`)))
	require.NoError(t, events.AppendBroadcastEvent(ctx, gwCOtherTenant, "gwc.OtherTenant", []byte(`{}`)))
	require.NoError(t, events.AppendDepartmentEvent(ctx, gwCOtherTenant, gwCDeptA, "gwc.OtherTenantDeptA", []byte(`{}`)))

	cases := []struct {
		name      string
		target    usecase.StreamTarget
		wantKey   string
		wantEvent string
	}{
		{"user", usecase.StreamTarget{TenantID: gwCTenant, UserID: gwCUser}, "sse:events:" + gwCTenant + ":" + gwCUser, "gwc.User"},
		{"other user", usecase.StreamTarget{TenantID: gwCTenant, UserID: gwCOtherUser}, "sse:events:" + gwCTenant + ":" + gwCOtherUser, "gwc.OtherUser"},
		{"tenant wide", usecase.StreamTarget{TenantID: gwCTenant}, "sse:events:broadcast:" + gwCTenant, "gwc.Tenant"},
		{"department A", usecase.StreamTarget{TenantID: gwCTenant, DepartmentID: gwCDeptA}, "sse:events:broadcast:" + gwCTenant + ":" + gwCDeptA, "gwc.DeptA"},
		{"department B", usecase.StreamTarget{TenantID: gwCTenant, DepartmentID: gwCDeptB}, "sse:events:broadcast:" + gwCTenant + ":" + gwCDeptB, "gwc.DeptB"},
		{"other tenant wide", usecase.StreamTarget{TenantID: gwCOtherTenant}, "sse:events:broadcast:" + gwCOtherTenant, "gwc.OtherTenant"},
		{"department A of the other tenant", usecase.StreamTarget{TenantID: gwCOtherTenant, DepartmentID: gwCDeptA}, "sse:events:broadcast:" + gwCOtherTenant + ":" + gwCDeptA, "gwc.OtherTenantDeptA"},
	}
	keys := map[string]bool{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantKey, streams.KeyFor(tc.target))
			length, err := client.XLen(ctx, tc.wantKey).Result()
			require.NoError(t, err)
			assert.EqualValues(t, 1, length, "the event is in the key named for its scope")
			ttl, err := client.TTL(ctx, tc.wantKey).Result()
			require.NoError(t, err)
			assert.Greater(t, ttl, 10*time.Minute)

			frames, err := streams.RangeSince(ctx, tc.target, "0-0")

			require.NoError(t, err)
			require.Len(t, frames, 1, "no other scope's event leaks into this read")
			assert.Equal(t, tc.wantEvent, frames[0].EventType)
			assert.Equal(t, tc.wantKey, frames[0].SourceKey)
		})
		keys[tc.wantKey] = true
	}
	stored, err := client.Keys(ctx, "sse:events:*").Result()
	require.NoError(t, err)
	assert.Len(t, keys, len(cases), "every scope has a distinct key")
	assert.ElementsMatch(t, mapKeys(keys), stored, "the writes created these keys and no others")
}

func mapKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// A resume is exclusive of the id it names, wherever in the stream that id is,
// and an id Valkey cannot parse is an error with no frames.
func TestGwCStreamStore_RangeSinceIsExclusiveAtAnyEntryAndRefusesAMalformedID(t *testing.T) {
	client := gwCClient(t)
	events := valkey.NewEventStore(client, 10*time.Minute, 15*time.Minute)
	streams := valkey.NewStreamStore(client)
	ctx := context.Background()
	target := usecase.StreamTarget{TenantID: gwCTenant, UserID: gwCUser}
	for _, name := range []string{"gwc.One", "gwc.Two", "gwc.Three"} {
		require.NoError(t, events.AppendEvent(ctx, gwCTenant, gwCUser, name, []byte(`{}`)))
	}
	ids := gwCEntryIDs(t, client, streams.KeyFor(target))
	require.Len(t, ids, 3)
	types := func(frames []domain.Frame) []string {
		out := make([]string, len(frames))
		for i, f := range frames {
			out[i] = f.EventType
		}
		return out
	}

	all, err := streams.RangeSince(ctx, target, "0-0")
	require.NoError(t, err)
	assert.Equal(t, []string{"gwc.One", "gwc.Two", "gwc.Three"}, types(all))
	fromMiddle, err := streams.RangeSince(ctx, target, ids[1])
	require.NoError(t, err)
	assert.Equal(t, []string{"gwc.Three"}, types(fromMiddle), "the entry named by the cursor is not repeated")
	fromLast, err := streams.RangeSince(ctx, target, ids[2])
	require.NoError(t, err)
	assert.Empty(t, fromLast, "nothing follows the newest entry")

	for _, bad := range []string{"abc", "", "1-x", "(1-0", "1-0-0"} {
		t.Run("malformed "+bad, func(t *testing.T) {
			frames, err := streams.RangeSince(ctx, target, bad)

			require.Error(t, err)
			assert.True(t, strings.HasPrefix(err.Error(), "valkey xrange: "), "got %q", err.Error())
			assert.Contains(t, err.Error(), "Invalid stream ID")
			assert.Nil(t, frames)
		})
	}
}

// With the release and the TTL refresh failing on an open stream the stream
// keeps delivering, the failed release reports its error, and the counter it
// leaves behind carries an expiry so it clears itself.
func TestGwCStream_AFailingRefreshAndReleaseLeaveTheStreamServingAndTheCounterExpiring(t *testing.T) {
	client := gwCClient(t)
	limiterClient := gwCSibling(t, client, "gwc-limiter")
	hook := &gwCHook{}
	limiterClient.AddHook(hook)
	reader := gwCSibling(t, client, "gwc-df03")
	const connTTL = time.Hour
	svc := usecase.NewStreamService(nil, valkey.NewStreamStore(reader), valkey.NewConnectionLimiter(limiterClient), 3, connTTL, time.Minute)
	ctx := context.Background()
	counter := "sse:conncount:" + gwCTenant + ":" + gwCUser

	granted, err := svc.AcquireConnection(ctx, gwCTenant, gwCUser)
	require.NoError(t, err)
	require.True(t, granted)

	hook.fail("expire", "eval", "evalsha")
	types := gwCServeOneEvent(t, svc, client, "gwc-df03", domain.Ticket{TenantID: gwCTenant, UserID: gwCUser}, "gwc.Delivered")
	assert.Equal(t, []string{"gwc.Delivered"}, types, "the frame arrived although the refresh failed")
	assert.GreaterOrEqual(t, hook.count("expire"), 1, "the refresh was attempted and failed")

	err = svc.ReleaseConnection(ctx, gwCTenant, gwCUser)
	require.Error(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "valkey release: "), "got %q", err.Error())
	assert.Contains(t, err.Error(), "injected")
	count, err := client.Get(ctx, counter).Result()
	require.NoError(t, err)
	assert.Equal(t, "1", count, "the counter is left behind")
	pttl, err := client.PTTL(ctx, counter).Result()
	require.NoError(t, err)
	assert.Greater(t, pttl, time.Duration(0), "the stale counter has an expiry")
	assert.LessOrEqual(t, pttl, connTTL)

	hook.fail()
	require.NoError(t, svc.ReleaseConnection(ctx, gwCTenant, gwCUser))
	exists, err := client.Exists(ctx, counter).Result()
	require.NoError(t, err)
	assert.Zero(t, exists, "once Valkey answers, the release removes the counter")
}

// A ticket is read with GET, not GETDEL, so it can open a second stream while
// it lives, and reading it never shortens its life.
func TestGwCTicket_ReusedWithinItsTTLOpensASecondStreamAndTheKeySurvives(t *testing.T) {
	client := gwCClient(t)
	ticketClient := gwCSibling(t, client, "gwc-tickets")
	hook := &gwCHook{}
	ticketClient.AddHook(hook)
	const ticketTTL = time.Hour
	tickets := usecase.NewTicketService(valkey.NewTicketStore(ticketClient), ticketTTL, 10, time.Minute)
	reader := gwCSibling(t, client, "gwc-sec01")
	svc := usecase.NewStreamService(tickets, valkey.NewStreamStore(reader), valkey.NewConnectionLimiter(client), 3, time.Minute, time.Minute)
	ctx := context.Background()
	snapshot := domain.Ticket{TenantID: gwCTenant, UserID: gwCUser, TenantRoles: []string{"member"}, IssuedAt: time.Now().UTC().Truncate(time.Second)}

	token, _, err := tickets.Mint(ctx, snapshot)
	require.NoError(t, err)
	for _, attempt := range []string{"first", "second"} {
		ticket, err := svc.ValidateTicket(ctx, token)
		require.NoError(t, err, attempt)
		assert.Equal(t, snapshot, ticket, attempt)

		granted, err := svc.AcquireConnection(ctx, gwCTenant, gwCUser)
		require.NoError(t, err, attempt)
		require.True(t, granted, attempt)
		types := gwCServeOneEvent(t, svc, client, "gwc-sec01", ticket, "gwc.Reused."+attempt)
		assert.Equal(t, []string{"gwc.Reused." + attempt}, types, "the %s stream opened and delivered", attempt)
		require.NoError(t, svc.ReleaseConnection(ctx, gwCTenant, gwCUser), attempt)
	}

	assert.Equal(t, 2, hook.count("get"), "each validation is one GET: %v", hook.seenNames())
	assert.Zero(t, hook.count("getdel")+hook.count("del")+hook.count("getex"), "reading never removes or re-times the ticket")
	pttl, err := client.PTTL(ctx, "ssegw:ticket:"+token).Result()
	require.NoError(t, err, "the ticket is still stored")
	assert.Greater(t, pttl, time.Duration(0))
	assert.LessOrEqual(t, pttl, ticketTTL, "the lifetime was not extended")
}
