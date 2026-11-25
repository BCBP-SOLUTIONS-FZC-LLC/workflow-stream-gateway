//go:build integration

package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/outbound/valkey"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability/obstest"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

func gwBStartValkey(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "valkey/valkey:8",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor: wait.ForAll(
				wait.ForListeningPort("6379/tcp"),
				wait.ForLog("Ready to accept connections"),
			).WithDeadline(60 * time.Second),
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })
	endpoint, err := container.Endpoint(ctx, "")
	require.NoError(t, err)
	return endpoint
}

const (
	gwBFramesMetric = "workflow_stream_gateway_frames_written_total"
	gwBTicketKey    = "ssegw:ticket:tok"
	gwBCountKey     = "sse:conncount:" + testTenantID + ":" + testUserID
	gwBSeedTTL      = 30 * time.Second
)

// gwBRealWiring builds the production router on a real Valkey: real ticket store, connection
// limiter and event store. The stream store is a spy that proves Serve was never reached.
func gwBRealWiring(t *testing.T) (*gin.Engine, *redis.Client, *gwBStreamStore) {
	t.Helper()
	return gwBRealWiringStopping(t, context.Background())
}

func gwBRealWiringStopping(t *testing.T, stopping context.Context) (*gin.Engine, *redis.Client, *gwBStreamStore) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	client := redis.NewClient(&redis.Options{Addr: gwBStartValkey(t)})
	t.Cleanup(func() { _ = client.Close() })
	tickets := usecase.NewTicketService(valkey.NewTicketStore(client), 60*time.Second, 10, time.Minute)
	stream := &gwBStreamStore{}
	r := NewRouter(RouterConfig{
		GinConfig:     gwBGinConfig,
		AppEnv:        "test",
		ValkeyClient:  client,
		TicketHandler: NewTicketHandler(tickets),
		EventsHandler: NewEventsHandler(usecase.NewFanoutService(valkey.NewEventStore(client, time.Hour, time.Hour), gwBRoutes(), usecase.CallerCheckOn)),
		StreamHandler: NewStreamHandler(usecase.NewStreamService(tickets, stream, valkey.NewConnectionLimiter(client), gwBConnCap, gwBConnTTL, time.Millisecond), stopping),
	})
	return r, client, stream
}

func gwBServe(r *gin.Engine, req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

// A ticket whose stored JSON cannot be decoded is treated like a store failure: 503, counted as
// valkey_unavailable, and no connection slot is taken.
func TestGwB_Integration_Stream_ACorruptStoredTicketIsAnUnavailableProblem(t *testing.T) {
	obstest.Init(t)
	r, client, stream := gwBRealWiring(t)
	ctx := context.Background()
	require.NoError(t, client.Set(ctx, gwBTicketKey, "{not json", time.Minute).Err())
	labels := map[string]string{"reason": "valkey_unavailable"}
	before := obstest.Sum(t, gwBRejectedMetric, labels)

	rec := gwBServe(r, httptest.NewRequest(http.MethodGet, gwBStreamPath+"?ticket=tok", nil))

	gwBAssertProblem(t, rec, http.StatusServiceUnavailable, "unavailable", "ticket store unavailable")
	assert.Equal(t, before+1, obstest.Sum(t, gwBRejectedMetric, labels))
	n, err := client.Exists(ctx, gwBCountKey).Result()
	require.NoError(t, err)
	assert.Zero(t, n, "no connection slot was taken")
	assert.Zero(t, stream.latestCalls+stream.readCalls, "Serve was never called")
}

// The acquire script is one atomic call: when it fails (here, on a counter that is not a number)
// the handler answers 503, and the counter and its expiry are exactly as they were.
func TestGwB_Integration_Stream_AFailingAcquireScriptIsAnUnavailableProblemWithTheCounterUntouched(t *testing.T) {
	obstest.Init(t)
	r, client, stream := gwBRealWiring(t)
	ctx := context.Background()
	require.NoError(t, client.Set(ctx, gwBTicketKey, `{"tenant_id":"`+testTenantID+`","user_id":"`+testUserID+`"}`, time.Minute).Err())
	require.NoError(t, client.Set(ctx, gwBCountKey, "not-a-number", gwBSeedTTL).Err())
	labels := map[string]string{"reason": "valkey_unavailable"}
	before := obstest.Sum(t, gwBRejectedMetric, labels)
	openBefore := obstest.Sum(t, gwBOpenMetric, nil)

	rec := gwBServe(r, httptest.NewRequest(http.MethodGet, gwBStreamPath+"?ticket=tok", nil))

	gwBAssertProblem(t, rec, http.StatusServiceUnavailable, "unavailable", "connection store unavailable")
	assert.Equal(t, before+1, obstest.Sum(t, gwBRejectedMetric, labels))
	assert.Equal(t, openBefore, obstest.Sum(t, gwBOpenMetric, nil))
	value, err := client.Get(ctx, gwBCountKey).Result()
	require.NoError(t, err)
	assert.Equal(t, "not-a-number", value, "the script did not increment")
	ttl, err := client.TTL(ctx, gwBCountKey).Result()
	require.NoError(t, err)
	assert.LessOrEqual(t, ttl, gwBSeedTTL, "the script did not extend the expiry")
	assert.Zero(t, stream.latestCalls+stream.readCalls, "Serve was never called")
}

// With the counter already at the cap the real script answers 0 without incrementing or
// extending the key, and the handler refuses with a 429 and no Retry-After.
func TestGwB_Integration_Stream_ACounterSeededAtTheCapIsRefusedAndLeftAsItWas(t *testing.T) {
	obstest.Init(t)
	r, client, stream := gwBRealWiring(t)
	ctx := context.Background()
	require.NoError(t, client.Set(ctx, gwBTicketKey, `{"tenant_id":"`+testTenantID+`","user_id":"`+testUserID+`"}`, time.Minute).Err())
	require.NoError(t, client.Set(ctx, gwBCountKey, gwBConnCap, gwBSeedTTL).Err())
	labels := map[string]string{"reason": "cap_exceeded"}
	before := obstest.Sum(t, gwBRejectedMetric, labels)
	openBefore := obstest.Sum(t, gwBOpenMetric, nil)

	rec := gwBServe(r, httptest.NewRequest(http.MethodGet, gwBStreamPath+"?ticket=tok", nil))

	gwBAssertProblem(t, rec, http.StatusTooManyRequests, "connection-cap-exceeded", "concurrent connection cap exceeded")
	assert.Empty(t, rec.Header().Get("Retry-After"))
	assert.Equal(t, before+1, obstest.Sum(t, gwBRejectedMetric, labels))
	assert.Equal(t, openBefore, obstest.Sum(t, gwBOpenMetric, nil))
	count, err := client.Get(ctx, gwBCountKey).Int()
	require.NoError(t, err)
	assert.Equal(t, gwBConnCap, count, "the counter stays at the cap")
	ttl, err := client.TTL(ctx, gwBCountKey).Result()
	require.NoError(t, err)
	assert.LessOrEqual(t, ttl, gwBSeedTTL, "a refused connection does not extend the counter's expiry")
	assert.Zero(t, stream.latestCalls+stream.readCalls, "Serve was never called")
}

// An accepted ingest on the real event store counts one ingest under the type's label and one
// frame per Valkey Stream entry written, and the entries carry the stream event type and the raw
// envelope.
func TestGwB_Integration_EventsIngest_CountsTheIngestAndEachFrameWritten(t *testing.T) {
	obstest.Init(t)
	r, client, _ := gwBRealWiring(t)
	ctx := context.Background()
	ingest := map[string]string{"event_type": gwBCreatedType, "result": "ok"}
	frames := map[string]string{"event_type": gwBCreatedType}
	ingestBefore := obstest.Sum(t, gwBIngestMetric, ingest)
	framesBefore := obstest.Sum(t, gwBFramesMetric, frames)
	framesAllBefore := obstest.Sum(t, gwBFramesMetric, nil)

	rec := gwBServe(r, gwBSystemPost(gwBEventsPath, gwBValidEnvelope))

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, ingestBefore+1, obstest.Sum(t, gwBIngestMetric, ingest))
	assert.Equal(t, framesBefore+3, obstest.Sum(t, gwBFramesMetric, frames), "two recipients plus the tenant-admin broadcast")
	assert.Equal(t, framesAllBefore+3, obstest.Sum(t, gwBFramesMetric, nil), "no other event type moved")
	for _, key := range []string{
		"sse:events:" + testTenantID + ":00000000-0000-4000-8000-0000000000b1",
		"sse:events:" + testTenantID + ":00000000-0000-4000-8000-0000000000b2",
		"sse:events:broadcast:" + testTenantID,
	} {
		entries, err := client.XRange(ctx, key, "-", "+").Result()
		require.NoError(t, err)
		require.Len(t, entries, 1, key)
		assert.Equal(t, gwBCreatedType, entries[0].Values["event"], key)
		assert.JSONEq(t, gwBValidEnvelope, entries[0].Values["data"].(string), key)
	}
}

// Shutdown ends an open stream on the real stores: while it runs the user's counter holds one
// slot and the gauge counts it; once the handler returns the counter key is gone and the gauge
// is back.
func TestGwB_Integration_Stream_ShutdownReleasesTheSlotInValkey(t *testing.T) {
	obstest.Init(t)
	stopping, beginShutdown := context.WithCancel(context.Background())
	t.Cleanup(beginShutdown)
	r, client, stream := gwBRealWiringStopping(t, stopping)
	ctx := context.Background()
	require.NoError(t, client.Set(ctx, gwBTicketKey, `{"tenant_id":"`+testTenantID+`","user_id":"`+testUserID+`"}`, time.Minute).Err())
	openBefore := obstest.Sum(t, gwBOpenMetric, nil)
	var countWhileOpen string
	var openWhileRunning float64
	stream.readNew = func(readCtx context.Context, _ []usecase.StreamPosition) ([]domain.Frame, error) {
		countWhileOpen, _ = client.Get(ctx, gwBCountKey).Result()
		openWhileRunning = obstest.Sum(t, gwBOpenMetric, nil)
		beginShutdown()
		<-readCtx.Done()
		return nil, readCtx.Err()
	}

	rec := gwBServe(r, httptest.NewRequest(http.MethodGet, gwBStreamPath+"?ticket=tok", nil))

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "1", countWhileOpen, "the open stream holds one slot")
	assert.Equal(t, openBefore+1, openWhileRunning)
	assert.Equal(t, openBefore, obstest.Sum(t, gwBOpenMetric, nil))
	n, err := client.Exists(ctx, gwBCountKey).Result()
	require.NoError(t, err)
	assert.Zero(t, n, "the counter key is deleted once the last slot is released")
}
