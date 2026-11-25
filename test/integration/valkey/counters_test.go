//go:build integration

package valkey_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/outbound/valkey"
)

// failFirstExpire makes the first EXPIRE sent through the client fail, the
// way a Valkey hiccup between two commands would.
type failFirstExpire struct{ failed atomic.Bool }

func (h *failFirstExpire) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (h *failFirstExpire) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if strings.EqualFold(cmd.Name(), "expire") && h.failed.CompareAndSwap(false, true) {
			err := errors.New("injected expire failure")
			cmd.SetErr(err)
			return err
		}
		return next(ctx, cmd)
	}
}

func (h *failFirstExpire) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestTicketStore_TheMintRateCounterAlwaysCarriesItsWindow(t *testing.T) {
	client := newClient(t)
	client.AddHook(&failFirstExpire{})
	store := valkey.NewTicketStore(client)
	ctx := context.Background()

	_, _ = store.AllowMint(ctx, "t1", "u1", 10, time.Minute)
	_, _ = store.AllowMint(ctx, "t1", "u1", 10, time.Minute)

	ttl, err := client.TTL(ctx, "sse:ticketrate:t1:u1").Result()
	require.NoError(t, err)
	assert.Greater(t, ttl, time.Duration(0), "a counter without an expiry would refuse the user for good once it reaches the limit")
}

func TestConnectionLimiter_AFailedAcquireNeverLeavesACounterWithoutExpiry(t *testing.T) {
	client := newClient(t)
	client.AddHook(&failFirstExpire{})
	limiter := valkey.NewConnectionLimiter(client)
	ctx := context.Background()

	_, _ = limiter.TryAcquire(ctx, "t1", "u1", 3, time.Minute)

	ttl, err := client.TTL(ctx, "sse:conncount:t1:u1").Result()
	require.NoError(t, err)
	assert.NotEqual(t, time.Duration(-1), ttl, "a counter held with no expiry and no release counts a connection that never opened")
}
