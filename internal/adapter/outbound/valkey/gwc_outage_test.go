package valkey

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

const (
	gwCTenant = "00000000-0000-4000-8000-0000000000c1"
	gwCUser   = "00000000-0000-4000-8000-0000000000c2"
	gwCDept   = "00000000-0000-4000-8000-0000000000c3"
)

// A closed client fails every command locally, which stands in for a Valkey
// that has gone away. Each adapter call must hand back a zero result and an
// error that starts with the call's own prefix and still wraps the cause.
func TestGwCAdapters_AFailingClientIsAWrappedErrorWithAZeroResult(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	require.NoError(t, client.Close())
	ctx := context.Background()
	limiter := NewConnectionLimiter(client)
	tickets := NewTicketStore(client)
	events := NewEventStore(client, time.Minute, 2*time.Minute)

	cases := []struct {
		name       string
		call       func(t *testing.T) error
		wantPrefix string
	}{
		{
			name: "acquire is not a grant",
			call: func(t *testing.T) error {
				granted, err := limiter.TryAcquire(ctx, gwCTenant, gwCUser, 3, time.Minute)
				assert.False(t, granted, "an error is never a grant")
				return err
			},
			wantPrefix: "valkey acquire: ",
		},
		{
			name:       "release",
			call:       func(t *testing.T) error { return limiter.Release(ctx, gwCTenant, gwCUser) },
			wantPrefix: "valkey release: ",
		},
		{
			name:       "refresh",
			call:       func(t *testing.T) error { return limiter.Refresh(ctx, gwCTenant, gwCUser, time.Minute) },
			wantPrefix: "valkey expire: ",
		},
		{
			name: "mint rate check is not an allowance",
			call: func(t *testing.T) error {
				allowed, err := tickets.AllowMint(ctx, gwCTenant, gwCUser, 1000, time.Minute)
				assert.False(t, allowed, "an error is never an allowance")
				return err
			},
			wantPrefix: "valkey mint rate: ",
		},
		{
			name: "ticket put",
			call: func(t *testing.T) error {
				return tickets.PutTicket(ctx, "tok", domain.Ticket{TenantID: gwCTenant, UserID: gwCUser}, time.Minute)
			},
			wantPrefix: "valkey set: ",
		},
		{
			name: "ticket get is not a missing ticket",
			call: func(t *testing.T) error {
				ticket, err := tickets.GetTicket(ctx, "tok")
				assert.Equal(t, domain.Ticket{}, ticket)
				assert.NotErrorIs(t, err, usecase.ErrTicketNotFound, "an outage must not look like an expired ticket")
				return err
			},
			wantPrefix: "valkey get: ",
		},
		{
			name:       "personal event append",
			call:       func(t *testing.T) error { return events.AppendEvent(ctx, gwCTenant, gwCUser, "x", []byte(`{}`)) },
			wantPrefix: "valkey xadd: ",
		},
		{
			name:       "broadcast event append",
			call:       func(t *testing.T) error { return events.AppendBroadcastEvent(ctx, gwCTenant, "x", []byte(`{}`)) },
			wantPrefix: "valkey xadd: ",
		},
		{
			name: "department event append",
			call: func(t *testing.T) error {
				return events.AppendDepartmentEvent(ctx, gwCTenant, gwCDept, "x", []byte(`{}`))
			},
			wantPrefix: "valkey xadd: ",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call(t)

			require.Error(t, err)
			assert.ErrorIs(t, err, redis.ErrClosed, "the client's own error stays reachable")
			assert.True(t, strings.HasPrefix(err.Error(), tc.wantPrefix), "the message starts with the call's prefix, got %q", err.Error())
		})
	}
}

// The stream key is chosen from the target: a user wins over a department, a
// department over the tenant-wide stream.
func TestGwCKeyFor_TheTargetPicksTheStreamKey(t *testing.T) {
	cases := []struct {
		name   string
		target usecase.StreamTarget
		want   string
	}{
		{"user", usecase.StreamTarget{TenantID: gwCTenant, UserID: gwCUser}, "sse:events:" + gwCTenant + ":" + gwCUser},
		{"department", usecase.StreamTarget{TenantID: gwCTenant, DepartmentID: gwCDept}, "sse:events:broadcast:" + gwCTenant + ":" + gwCDept},
		{"tenant wide", usecase.StreamTarget{TenantID: gwCTenant}, "sse:events:broadcast:" + gwCTenant},
		{"a user and a department name the user's stream", usecase.StreamTarget{TenantID: gwCTenant, UserID: gwCUser, DepartmentID: gwCDept}, "sse:events:" + gwCTenant + ":" + gwCUser},
	}
	store := NewStreamStore(nil)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, keyFor(tc.target))
			assert.Equal(t, tc.want, store.KeyFor(tc.target))
		})
	}
}
