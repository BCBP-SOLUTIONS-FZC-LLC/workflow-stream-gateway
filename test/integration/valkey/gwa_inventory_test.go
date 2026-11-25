//go:build integration

package valkey_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/outbound/routingfile"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/outbound/valkey"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability/obstest"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

const (
	gwATenant      = "00000000-0000-4000-8000-00000000a001"
	gwAOtherTenant = "00000000-0000-4000-8000-00000000a002"
	gwAUser        = "00000000-0000-4000-8000-00000000b001"
	gwAOtherUser   = "00000000-0000-4000-8000-00000000b002"
	gwAKey         = "sse:events:" + gwATenant + ":" + gwAUser
	gwAForeignKey  = "sse:events:" + gwAOtherTenant + ":" + gwAOtherUser
)

type gwAFrame struct {
	id, eventType string
	data          []byte
}

// gwASink records what Serve writes and runs a callback after each write, so a
// test ends the stream by construction once the frame it waits for has arrived.
type gwASink struct {
	frames    []gwAFrame
	control   []gwAFrame
	onFrame   func(written int)
	onControl func()
}

func (s *gwASink) WriteFrame(id, eventType string, data []byte) error {
	s.frames = append(s.frames, gwAFrame{id, eventType, data})
	if s.onFrame != nil {
		s.onFrame(len(s.frames))
	}
	return nil
}

func (s *gwASink) WriteControlFrame(eventType string, data []byte) error {
	s.control = append(s.control, gwAFrame{eventType: eventType, data: data})
	if s.onControl != nil {
		s.onControl()
	}
	return nil
}

func (s *gwASink) WriteHeartbeat() error { return nil }

// gwAXReadLog records the arguments of every XREAD a client sends.
type gwAXReadLog struct {
	mu    sync.Mutex
	calls [][]any
}

func (h *gwAXReadLog) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *gwAXReadLog) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if strings.EqualFold(cmd.Name(), "xread") {
			h.mu.Lock()
			h.calls = append(h.calls, append([]any(nil), cmd.Args()...))
			h.mu.Unlock()
		}
		return next(ctx, cmd)
	}
}

func (h *gwAXReadLog) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *gwAXReadLog) snapshot() [][]any {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([][]any(nil), h.calls...)
}

// gwAAfterLatest runs a callback once, right after the newest-entry lookup of a
// connection returns: the moment between "connect" and the first live read.
type gwAAfterLatest struct {
	usecase.StreamStore
	after func()
}

func (s *gwAAfterLatest) LatestEntryID(ctx context.Context, t usecase.StreamTarget) (string, error) {
	id, err := s.StreamStore.LatestEntryID(ctx, t)
	if err == nil && s.after != nil {
		run := s.after
		s.after = nil
		run()
	}
	return id, err
}

func gwABlockedInXRead(client *redis.Client, name string) bool {
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

func gwAEntryIDs(t *testing.T, client *redis.Client, key string) []string {
	t.Helper()
	entries, err := client.XRange(context.Background(), key, "-", "+").Result()
	require.NoError(t, err)
	ids := make([]string, len(entries))
	for i, e := range entries {
		ids[i] = e.ID
	}
	return ids
}

func gwAService(reader *redis.Client, client *redis.Client) *usecase.StreamService {
	return usecase.NewStreamService(nil, valkey.NewStreamStore(reader), valkey.NewConnectionLimiter(client), 3, time.Minute, 200*time.Millisecond)
}

// --- I-1-HP-01 ------------------------------------------------------------------

func TestGwAFanout_AnIngestedEventIsWrittenWithItsFieldsExpiryAndMetric(t *testing.T) {
	obstest.Init(t)
	client := newClient(t)
	routes, err := routingfile.Load("../../../config/routing.yaml")
	require.NoError(t, err)
	const keyTTL = 15 * time.Minute
	fanout := usecase.NewFanoutService(valkey.NewEventStore(client, 10*time.Minute, keyTTL), routes, usecase.CallerCheckOff)
	ctx := context.Background()
	envelope := []byte(`{"type":"WorkflowTaskCreated","tenant_id":"` + gwATenant + `","note":"line one\nline two"}`)
	payload := []byte(`{"recipients":[{"kind":"user","id":"` + gwAUser + `"},{"kind":"user","id":"` + gwAOtherUser + `"}]}`)
	labels := map[string]string{"event_type": "workflow.WorkflowTaskCreated"}
	before := obstest.Sum(t, "workflow_stream_gateway_frames_written_total", labels)

	name, err := fanout.Ingest(ctx, usecase.IngestInput{
		Source: "workflow-execution-svc", EventType: "WorkflowTaskCreated", TenantID: gwATenant, Payload: payload, RawEnvelope: envelope,
	})

	require.NoError(t, err)
	assert.Equal(t, "workflow.WorkflowTaskCreated", name)
	keys := []string{
		"sse:events:" + gwATenant + ":" + gwAUser,
		"sse:events:" + gwATenant + ":" + gwAOtherUser,
		"sse:events:broadcast:" + gwATenant,
	}
	stored, err := client.Keys(ctx, "sse:events:*").Result()
	require.NoError(t, err)
	assert.ElementsMatch(t, keys, stored, "one stream per user recipient and the tenant administrators' stream, nothing else")
	for _, key := range keys {
		entries, err := client.XRange(ctx, key, "-", "+").Result()
		require.NoError(t, err)
		require.Len(t, entries, 1, key)
		assert.Equal(t, map[string]any{"event": "workflow.WorkflowTaskCreated", "data": string(envelope)}, entries[0].Values, "%s holds exactly the event and data fields, the data being the envelope as received", key)
		ttl, err := client.TTL(ctx, key).Result()
		require.NoError(t, err)
		assert.Greater(t, ttl, time.Duration(0), "%s expires", key)
		assert.LessOrEqual(t, ttl, keyTTL, "%s expires within the configured key lifetime", key)
	}
	assert.Equal(t, before+3, obstest.Sum(t, "workflow_stream_gateway_frames_written_total", labels), "one frames-written count per stream entry")
}

// --- P-2-BL-04 ------------------------------------------------------------------

func TestGwAStreamStore_ALiveReadReturnsAtMost100EntriesAfterTheGivenPosition(t *testing.T) {
	client := newClient(t)
	reads := &gwAXReadLog{}
	reader := redis.NewClient(&redis.Options{Addr: client.Options().Addr})
	t.Cleanup(func() { _ = reader.Close() })
	reader.AddHook(reads)
	events := valkey.NewEventStore(client, 10*time.Minute, 15*time.Minute)
	streams := valkey.NewStreamStore(reader)
	ctx := context.Background()
	target := usecase.StreamTarget{TenantID: gwATenant, UserID: gwAUser}
	for range 150 {
		require.NoError(t, events.AppendEvent(ctx, gwATenant, gwAUser, "gwa.Bulk", []byte(`{}`)))
	}
	ids := gwAEntryIDs(t, client, gwAKey)
	require.Len(t, ids, 150)

	first, err := streams.ReadNew(ctx, []usecase.StreamPosition{{Target: target, AfterID: ids[9]}}, 50*time.Millisecond)
	require.NoError(t, err)
	require.Len(t, first, 100, "a read is capped at 100 entries")
	assert.Equal(t, ids[10], first[0].ID, "the read starts after the given position, which is excluded")
	assert.Equal(t, ids[109], first[99].ID)
	second, err := streams.ReadNew(ctx, []usecase.StreamPosition{{Target: target, AfterID: first[99].ID}}, 50*time.Millisecond)
	require.NoError(t, err)
	require.Len(t, second, 40, "the next read continues where the last stopped")
	assert.Equal(t, ids[110], second[0].ID)

	calls := reads.snapshot()
	require.Len(t, calls, 2)
	assert.Equal(t, []any{"xread", "count", int64(100), "block", int64(50), "streams", gwAKey, ids[9]}, calls[0])
	assert.Equal(t, []any{"xread", "count", int64(100), "block", int64(50), "streams", gwAKey, ids[109]}, calls[1])
}

func TestGwAStreamService_ALiveEventIsDeliveredWithItsTypeDataAndCompositeCursor(t *testing.T) {
	client := newClient(t)
	reads := &gwAXReadLog{}
	reader := redis.NewClient(&redis.Options{Addr: client.Options().Addr, ClientName: "gwa-live"})
	t.Cleanup(func() { _ = reader.Close() })
	reader.AddHook(reads)
	events := valkey.NewEventStore(client, 10*time.Minute, 15*time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, events.AppendEvent(ctx, gwATenant, gwAUser, "gwa.BeforeConnect", []byte(`{"n":0}`)))
	beforeID := gwAEntryIDs(t, client, gwAKey)[0]
	sink := &gwASink{onFrame: func(int) { cancel() }}
	served := make(chan error, 1)
	go func() {
		served <- gwAService(reader, client).Serve(ctx, domain.Ticket{TenantID: gwATenant, UserID: gwAUser}, "", sink)
	}()

	require.Eventually(t, func() bool { return gwABlockedInXRead(client, "gwa-live") }, 10*time.Second, 5*time.Millisecond, "the stream waits in its first live read")
	envelope := []byte(`{"type":"WorkflowTaskCreated","n":1}`)
	require.NoError(t, events.AppendEvent(context.Background(), gwATenant, gwAUser, "workflow.WorkflowTaskCreated", envelope))
	require.NoError(t, <-served)

	ids := gwAEntryIDs(t, client, gwAKey)
	require.Len(t, ids, 2)
	require.Len(t, sink.frames, 1, "the event written before the connection is not delivered")
	assert.Equal(t, "workflow.WorkflowTaskCreated", sink.frames[0].eventType)
	assert.Equal(t, envelope, sink.frames[0].data)
	assert.Equal(t, gwAKey+"="+ids[1], sink.frames[0].id, "the frame id is the composite cursor naming the stream and the entry")
	assert.Empty(t, sink.control)
	calls := reads.snapshot()
	require.NotEmpty(t, calls)
	for _, call := range calls {
		require.Len(t, call, 8)
		assert.Equal(t, []any{"xread", "count", int64(100)}, call[:3])
		assert.Equal(t, gwAKey, call[6])
	}
	assert.Equal(t, beforeID, calls[0][7], "the first read starts after the newest entry that existed at connect, not at a dollar sign")
}

// --- P-2-EC-08 ------------------------------------------------------------------

func TestGwAStreamService_AnEventAddedBetweenConnectAndTheFirstReadIsTheFirstFrame(t *testing.T) {
	client := newClient(t)
	reader := redis.NewClient(&redis.Options{Addr: client.Options().Addr})
	t.Cleanup(func() { _ = reader.Close() })
	events := valkey.NewEventStore(client, 10*time.Minute, 15*time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, events.AppendEvent(ctx, gwATenant, gwAUser, "gwa.BeforeConnect", []byte(`{"n":0}`)))
	store := &gwAAfterLatest{StreamStore: valkey.NewStreamStore(reader), after: func() {
		require.NoError(t, events.AppendEvent(ctx, gwATenant, gwAUser, "gwa.AfterConnect", []byte(`{"n":1}`)))
	}}
	service := usecase.NewStreamService(nil, store, valkey.NewConnectionLimiter(client), 3, time.Minute, 200*time.Millisecond)
	sink := &gwASink{onFrame: func(int) { cancel() }}

	err := service.Serve(ctx, domain.Ticket{TenantID: gwATenant, UserID: gwAUser}, "", sink)

	require.NoError(t, err)
	ids := gwAEntryIDs(t, client, gwAKey)
	require.Len(t, ids, 2)
	require.Len(t, sink.frames, 1)
	assert.Equal(t, "gwa.AfterConnect", sink.frames[0].eventType, "the first data frame is the event added after connect")
	assert.JSONEq(t, `{"n":1}`, string(sink.frames[0].data))
	assert.Equal(t, gwAKey+"="+ids[1], sink.frames[0].id)
	assert.Empty(t, sink.control, "no backfill and no resync on a fresh connection")
}

// --- P-2-EC-05 ------------------------------------------------------------------

func TestGwAStreamService_AResumeAtTheOldestEntryReplaysAndOlderThanTheWindowResyncs(t *testing.T) {
	client := newClient(t)
	reader := redis.NewClient(&redis.Options{Addr: client.Options().Addr})
	t.Cleanup(func() { _ = reader.Close() })
	events := valkey.NewEventStore(client, 10*time.Minute, 15*time.Minute)
	ctx := context.Background()
	for _, name := range []string{"gwa.First", "gwa.Second", "gwa.Third"} {
		require.NoError(t, events.AppendEvent(ctx, gwATenant, gwAUser, name, []byte(`{}`)))
	}
	ids := gwAEntryIDs(t, client, gwAKey)
	require.Len(t, ids, 3)
	ticket := domain.Ticket{TenantID: gwATenant, UserID: gwAUser}

	t.Run("a cursor equal to the oldest entry replays everything after it", func(t *testing.T) {
		serveCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		sink := &gwASink{onFrame: func(written int) {
			if written == 2 {
				cancel()
			}
		}}

		err := gwAService(reader, client).Serve(serveCtx, ticket, gwAKey+"="+ids[0], sink)

		require.NoError(t, err)
		require.Len(t, sink.frames, 2)
		assert.Equal(t, "gwa.Second", sink.frames[0].eventType)
		assert.Equal(t, gwAKey+"="+ids[1], sink.frames[0].id)
		assert.Equal(t, "gwa.Third", sink.frames[1].eventType)
		assert.Equal(t, gwAKey+"="+ids[2], sink.frames[1].id)
		assert.Empty(t, sink.control, "equal ids are a replay, not a resync")
	})

	t.Run("a cursor older than the oldest entry resyncs and replays nothing", func(t *testing.T) {
		serveCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		sink := &gwASink{onControl: cancel}

		err := gwAService(reader, client).Serve(serveCtx, ticket, gwAKey+"=1-0", sink)

		require.NoError(t, err)
		assert.Empty(t, sink.frames)
		require.Len(t, sink.control, 1)
		assert.Equal(t, "resync-required", sink.control[0].eventType)
		assert.JSONEq(t, `{"reason":"backfill_window_exceeded"}`, string(sink.control[0].data))
	})
}

// --- P-2-MT-01 ------------------------------------------------------------------

func TestGwAStreamService_ACursorNamingAnotherTenantsStreamReplaysNothingFromIt(t *testing.T) {
	client := newClient(t)
	reader := redis.NewClient(&redis.Options{Addr: client.Options().Addr})
	t.Cleanup(func() { _ = reader.Close() })
	events := valkey.NewEventStore(client, 10*time.Minute, 15*time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, name := range []string{"gwa.ForeignOne", "gwa.ForeignTwo"} {
		require.NoError(t, events.AppendEvent(ctx, gwAOtherTenant, gwAOtherUser, name, []byte(`{"secret":true}`)))
	}
	store := &gwAAfterLatest{StreamStore: valkey.NewStreamStore(reader), after: func() {
		require.NoError(t, events.AppendEvent(ctx, gwATenant, gwAUser, "gwa.Own", []byte(`{"n":1}`)))
	}}
	service := usecase.NewStreamService(nil, store, valkey.NewConnectionLimiter(client), 3, time.Minute, 200*time.Millisecond)
	sink := &gwASink{onFrame: func(int) { cancel() }}

	err := service.Serve(ctx, domain.Ticket{TenantID: gwATenant, UserID: gwAUser}, gwAForeignKey+"=0-0", sink)

	require.NoError(t, err)
	require.Len(t, sink.frames, 1, "only the caller's own stream is read")
	assert.Equal(t, "gwa.Own", sink.frames[0].eventType)
	assert.NotContains(t, sink.frames[0].id, gwAForeignKey, "the resume cursor never names the foreign stream")
	assert.Empty(t, sink.control, "a foreign key is ignored, so it cannot even trigger a resync")
}
