//go:build integration

package valkey_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/outbound/valkey"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

func TestTicketStore_PutGetAndRateWindow(t *testing.T) {
	client := newClient(t)
	store := valkey.NewTicketStore(client)
	ctx := context.Background()

	ticket := domain.Ticket{TenantID: "t1", UserID: "u1", TenantRoles: []string{"member"}, IssuedAt: time.Now().UTC().Truncate(time.Second)}
	require.NoError(t, store.PutTicket(ctx, "tok", ticket, time.Minute))

	got, err := store.GetTicket(ctx, "tok")
	require.NoError(t, err)
	assert.Equal(t, ticket, got)
	again, err := store.GetTicket(ctx, "tok")
	require.NoError(t, err)
	assert.Equal(t, ticket, again, "a ticket is reusable within its TTL")

	_, err = store.GetTicket(ctx, "missing")
	assert.ErrorIs(t, err, usecase.ErrTicketNotFound)

	for i := 1; i <= 3; i++ {
		ok, err := store.AllowMint(ctx, "t1", "u1", 2, time.Minute)
		require.NoError(t, err)
		assert.Equal(t, i <= 2, ok, "mint %d", i)
	}
	ttl, err := client.TTL(ctx, "sse:ticketrate:t1:u1").Result()
	require.NoError(t, err)
	assert.Greater(t, ttl, time.Duration(0), "the window's expiry is set once and never removed")
}

func TestEventStoreAndStreamStore_RoundTrip(t *testing.T) {
	client := newClient(t)
	events := valkey.NewEventStore(client, 10*time.Minute, 15*time.Minute)
	streams := valkey.NewStreamStore(client)
	ctx := context.Background()

	personal := usecase.StreamTarget{TenantID: "t1", UserID: "u1"}
	broadcast := usecase.StreamTarget{TenantID: "t1"}
	department := usecase.StreamTarget{TenantID: "t1", DepartmentID: "d1"}

	_, found, err := streams.OldestEntryID(ctx, personal)
	require.NoError(t, err)
	assert.False(t, found)

	require.NoError(t, events.AppendEvent(ctx, "t1", "u1", "WorkflowTaskCreated", []byte(`{"n":1}`)))
	require.NoError(t, events.AppendEvent(ctx, "t1", "u1", "WorkflowTaskCompleted", []byte(`{"n":2}`)))
	require.NoError(t, events.AppendBroadcastEvent(ctx, "t1", "WorkflowInstancePaused", []byte(`{"n":3}`)))
	require.NoError(t, events.AppendDepartmentEvent(ctx, "t1", "d1", "WorkflowTaskCreated", []byte(`{"n":4}`)))

	first, found, err := streams.OldestEntryID(ctx, personal)
	require.NoError(t, err)
	require.True(t, found)

	frames, err := streams.RangeSince(ctx, personal, first)
	require.NoError(t, err)
	require.Len(t, frames, 1, "RangeSince is exclusive of the resume id")
	assert.Equal(t, "WorkflowTaskCompleted", frames[0].EventType)
	assert.JSONEq(t, `{"n":2}`, string(frames[0].Data))
	assert.Equal(t, streams.KeyFor(personal), frames[0].SourceKey)

	for _, key := range []string{streams.KeyFor(personal), streams.KeyFor(broadcast), streams.KeyFor(department)} {
		ttl, err := client.TTL(ctx, key).Result()
		require.NoError(t, err)
		assert.Greater(t, ttl, 10*time.Minute, "%s outlives the retention window", key)
	}
}

func TestStreamStore_ReadNewBlocksForTheNextEntry(t *testing.T) {
	client := newClient(t)
	events := valkey.NewEventStore(client, 10*time.Minute, 15*time.Minute)
	streams := valkey.NewStreamStore(client)
	ctx := context.Background()
	personal := usecase.StreamTarget{TenantID: "t1", UserID: "u1"}

	empty, err := streams.ReadNew(ctx, []usecase.StreamTarget{personal}, 100*time.Millisecond)
	require.NoError(t, err)
	assert.Empty(t, empty)

	go func() {
		time.Sleep(200 * time.Millisecond)
		_ = events.AppendEvent(context.Background(), "t1", "u1", "WorkflowTaskCreated", []byte(`{"n":1}`))
	}()
	frames, err := streams.ReadNew(ctx, []usecase.StreamTarget{personal}, 3*time.Second)
	require.NoError(t, err)
	require.Len(t, frames, 1)
	assert.Equal(t, "WorkflowTaskCreated", frames[0].EventType)
}

func TestNewClient_FailsWhenValkeyIsUnreachable(t *testing.T) {
	_, err := valkey.NewClient(context.Background(), valkey.Options{Addr: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond})
	assert.Error(t, err)
}

func TestAdapters_SurfaceAValkeyOutage(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 100 * time.Millisecond, MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	target := usecase.StreamTarget{TenantID: "t1", UserID: "u1"}

	tickets := valkey.NewTicketStore(client)
	_, err := tickets.AllowMint(ctx, "t1", "u1", 1, time.Minute)
	assert.Error(t, err)
	assert.Error(t, tickets.PutTicket(ctx, "tok", domain.Ticket{}, time.Minute))
	_, err = tickets.GetTicket(ctx, "tok")
	assert.Error(t, err)

	limiter := valkey.NewConnectionLimiter(client)
	_, err = limiter.TryAcquire(ctx, "t1", "u1", 1, time.Minute)
	assert.Error(t, err)
	assert.Error(t, limiter.Release(ctx, "t1", "u1"))
	assert.Error(t, limiter.Refresh(ctx, "t1", "u1", time.Minute))

	events := valkey.NewEventStore(client, time.Minute, 2*time.Minute)
	assert.Error(t, events.AppendEvent(ctx, "t1", "u1", "x", []byte(`{}`)))

	streams := valkey.NewStreamStore(client)
	_, _, err = streams.OldestEntryID(ctx, target)
	assert.Error(t, err)
	_, err = streams.RangeSince(ctx, target, "0-0")
	assert.Error(t, err)
	_, err = streams.ReadNew(ctx, []usecase.StreamTarget{target}, 10*time.Millisecond)
	assert.Error(t, err)
}

func TestTicketStore_RejectsACorruptStoredTicket(t *testing.T) {
	client := newClient(t)
	store := valkey.NewTicketStore(client)
	ctx := context.Background()
	require.NoError(t, client.Set(ctx, "ssegw:ticket:bad", "not json", time.Minute).Err())

	_, err := store.GetTicket(ctx, "bad")
	assert.Error(t, err)
}
