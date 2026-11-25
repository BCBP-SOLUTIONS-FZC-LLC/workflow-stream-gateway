package valkey

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

// A closed client fails every command locally, which stands in for a Valkey
// that has gone away.
func TestGwAStreamStore_EveryReadWrapsTheClientFailure(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	require.NoError(t, client.Close())
	store := NewStreamStore(client)
	ctx := context.Background()
	target := usecase.StreamTarget{TenantID: "00000000-0000-4000-8000-00000000a001", UserID: "00000000-0000-4000-8000-00000000b001"}

	cases := []struct {
		name       string
		call       func() error
		wantPrefix string
	}{
		{
			name: "newest entry lookup",
			call: func() error {
				id, err := store.LatestEntryID(ctx, target)
				assert.Empty(t, id)
				return err
			},
			wantPrefix: "valkey xrevrange: ",
		},
		{
			name: "oldest entry lookup",
			call: func() error {
				id, exists, err := store.OldestEntryID(ctx, target)
				assert.Empty(t, id)
				assert.False(t, exists)
				return err
			},
			wantPrefix: "valkey xrange: ",
		},
		{
			name: "range since a cursor",
			call: func() error {
				frames, err := store.RangeSince(ctx, target, "100-0")
				assert.Nil(t, frames)
				return err
			},
			wantPrefix: "valkey xrange: ",
		},
		{
			name: "live read",
			call: func() error {
				frames, err := store.ReadNew(ctx, []usecase.StreamPosition{{Target: target, AfterID: "100-0"}}, time.Millisecond)
				assert.Nil(t, frames)
				return err
			},
			wantPrefix: "valkey xread: ",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()

			require.Error(t, err)
			assert.ErrorIs(t, err, redis.ErrClosed, "the client's own error stays reachable")
			assert.True(t, strings.HasPrefix(err.Error(), tc.wantPrefix), "the message starts with the call's prefix, got %q", err.Error())
		})
	}
}
