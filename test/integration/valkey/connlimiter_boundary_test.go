//go:build integration

package valkey_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/outbound/valkey"
)

func TestConnectionLimiter_TryAcquire_CapthConnectionGrantedNextRefused(t *testing.T) {
	cases := []struct {
		name string
		cap  int
	}{
		{name: "cap of three", cap: 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := newClient(t)
			limiter := valkey.NewConnectionLimiter(client)
			ctx := context.Background()

			for i := 1; i <= tc.cap; i++ {
				ok, err := limiter.TryAcquire(ctx, "t1", "u1", tc.cap, time.Minute)
				require.NoError(t, err)
				assert.True(t, ok, "connection %d of %d is granted", i, tc.cap)
			}
			ok, err := limiter.TryAcquire(ctx, "t1", "u1", tc.cap, time.Minute)
			require.NoError(t, err)
			assert.False(t, ok, "connection cap+1 is refused")

			count, err := client.Get(ctx, "sse:conncount:t1:u1").Int()
			require.NoError(t, err)
			assert.Equal(t, tc.cap, count)
		})
	}
}

func TestConnectionLimiter_TryAcquire_TwoRacersForTheLastSlot_OneWins(t *testing.T) {
	cases := []struct {
		name string
		cap  int
	}{
		{name: "one slot left", cap: 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := newClient(t)
			limiter := valkey.NewConnectionLimiter(client)
			ctx := context.Background()

			for range tc.cap - 1 {
				ok, err := limiter.TryAcquire(ctx, "t1", "u1", tc.cap, time.Minute)
				require.NoError(t, err)
				require.True(t, ok)
			}

			start := make(chan struct{})
			var granted, refused atomic.Int32
			var wg sync.WaitGroup
			for range 2 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					ok, err := limiter.TryAcquire(ctx, "t1", "u1", tc.cap, time.Minute)
					if err != nil {
						return
					}
					if ok {
						granted.Add(1)
					} else {
						refused.Add(1)
					}
				}()
			}
			close(start)
			wg.Wait()

			assert.EqualValues(t, 1, granted.Load())
			assert.EqualValues(t, 1, refused.Load())
			count, err := client.Get(ctx, "sse:conncount:t1:u1").Int()
			require.NoError(t, err)
			assert.Equal(t, tc.cap, count)
		})
	}
}

func TestConnectionLimiter_Release_LastConnectionClosed_DeletesTheKey(t *testing.T) {
	cases := []struct {
		name   string
		opened int
	}{
		{name: "single connection", opened: 1},
		{name: "two connections", opened: 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := newClient(t)
			limiter := valkey.NewConnectionLimiter(client)
			ctx := context.Background()
			key := "sse:conncount:t1:u1"

			for range tc.opened {
				ok, err := limiter.TryAcquire(ctx, "t1", "u1", 5, time.Minute)
				require.NoError(t, err)
				require.True(t, ok)
			}
			for i := 1; i < tc.opened; i++ {
				require.NoError(t, limiter.Release(ctx, "t1", "u1"))
				exists, err := client.Exists(ctx, key).Result()
				require.NoError(t, err)
				assert.EqualValues(t, 1, exists, "key stays while a stream is open")
			}
			require.NoError(t, limiter.Release(ctx, "t1", "u1"))

			exists, err := client.Exists(ctx, key).Result()
			require.NoError(t, err)
			assert.Zero(t, exists, "key is deleted, not left at 0")
		})
	}
}
