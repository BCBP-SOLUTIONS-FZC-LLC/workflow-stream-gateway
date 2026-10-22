//go:build integration

package valkey_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/outbound/valkey"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/test/fixtures"
)

func newClient(t *testing.T) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: fixtures.NewTestValkey(t)})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestConnectionLimiter_EnforcesTheCapAtomically(t *testing.T) {
	client := newClient(t)
	limiter := valkey.NewConnectionLimiter(client)
	ctx := context.Background()

	var granted atomic.Int32
	var wg sync.WaitGroup
	for range 40 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := limiter.TryAcquire(ctx, "t1", "u1", 5, time.Minute)
			if err == nil && ok {
				granted.Add(1)
			}
		}()
	}
	wg.Wait()

	assert.EqualValues(t, 5, granted.Load(), "exactly cap connections are granted under contention")
	count, err := client.Get(ctx, "sse:conncount:t1:u1").Int()
	require.NoError(t, err)
	assert.Equal(t, 5, count, "rejected attempts leave no residue in the counter")
}

func TestConnectionLimiter_ReleaseNeverGoesNegative(t *testing.T) {
	client := newClient(t)
	limiter := valkey.NewConnectionLimiter(client)
	ctx := context.Background()

	ok, err := limiter.TryAcquire(ctx, "t1", "u1", 2, time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, limiter.Release(ctx, "t1", "u1"))
	require.NoError(t, limiter.Release(ctx, "t1", "u1"))

	for range 2 {
		ok, err = limiter.TryAcquire(ctx, "t1", "u1", 2, time.Minute)
		require.NoError(t, err)
		assert.True(t, ok)
	}
	ok, err = limiter.TryAcquire(ctx, "t1", "u1", 2, time.Minute)
	require.NoError(t, err)
	assert.False(t, ok, "a stray release must not raise the effective cap")
}

func TestConnectionLimiter_RefreshKeepsALiveConnectionCounted(t *testing.T) {
	client := newClient(t)
	limiter := valkey.NewConnectionLimiter(client)
	ctx := context.Background()

	ok, err := limiter.TryAcquire(ctx, "t1", "u1", 5, 2*time.Second)
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, limiter.Refresh(ctx, "t1", "u1", time.Minute))

	ttl, err := client.TTL(ctx, "sse:conncount:t1:u1").Result()
	require.NoError(t, err)
	assert.Greater(t, ttl, 30*time.Second)
}

func TestConnectionLimiter_RefreshOfAMissingCounterCreatesNothing(t *testing.T) {
	client := newClient(t)
	limiter := valkey.NewConnectionLimiter(client)
	ctx := context.Background()

	require.NoError(t, limiter.Refresh(ctx, "t1", "ghost", time.Minute))

	exists, err := client.Exists(ctx, "sse:conncount:t1:ghost").Result()
	require.NoError(t, err)
	assert.Zero(t, exists)
}
