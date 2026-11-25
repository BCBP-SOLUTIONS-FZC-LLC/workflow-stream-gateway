//go:build integration

package valkey_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/outbound/valkey"
)

func TestTicketStore_AllowMint_FirstMintSetsWindowExpiry(t *testing.T) {
	client := newClient(t)
	store := valkey.NewTicketStore(client)
	ctx := context.Background()
	const key = "sse:ticketrate:t1:u1"

	before, err := client.Exists(ctx, key).Result()
	require.NoError(t, err)
	require.Zero(t, before)

	ok, err := store.AllowMint(ctx, "t1", "u1", 5, time.Minute)
	require.NoError(t, err)
	assert.True(t, ok)

	ttl, err := client.PTTL(ctx, key).Result()
	require.NoError(t, err)
	assert.Greater(t, ttl, time.Duration(0), "counter must expire")
	assert.LessOrEqual(t, ttl, time.Minute, "expiry is the window, not longer")
}

func TestTicketStore_AllowMint_SameUserInTwoTenants_CountsSeparately(t *testing.T) {
	client := newClient(t)
	store := valkey.NewTicketStore(client)
	ctx := context.Background()

	ok, err := store.AllowMint(ctx, "tenant-a", "user-1", 1, time.Minute)
	require.NoError(t, err)
	assert.True(t, ok)
	ok, err = store.AllowMint(ctx, "tenant-a", "user-1", 1, time.Minute)
	require.NoError(t, err)
	assert.False(t, ok, "tenant-a is over its limit")

	ok, err = store.AllowMint(ctx, "tenant-b", "user-1", 1, time.Minute)
	require.NoError(t, err)
	assert.True(t, ok, "tenant-b has its own counter")

	for _, key := range []string{"sse:ticketrate:tenant-a:user-1", "sse:ticketrate:tenant-b:user-1"} {
		n, err := client.Exists(ctx, key).Result()
		require.NoError(t, err)
		assert.EqualValues(t, 1, n, key)
	}
}
