package usecase

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
)

// fakeStore is an in-memory Store for exercising TicketService without
// Valkey — mint counts and tickets are tracked per key, mirroring the real
// adapter's key shapes closely enough for these tests' purposes.
type fakeStore struct {
	mintCounts  map[string]int
	tickets     map[string]domain.Ticket
	forceErr    error
	forcePutErr error
}

func newFakeStore() *fakeStore {
	return &fakeStore{mintCounts: map[string]int{}, tickets: map[string]domain.Ticket{}}
}

func (f *fakeStore) AllowMint(_ context.Context, tenantID, userID string, max int, _ time.Duration) (bool, error) {
	if f.forceErr != nil {
		return false, f.forceErr
	}
	key := tenantID + ":" + userID
	f.mintCounts[key]++
	return f.mintCounts[key] <= max, nil
}

func (f *fakeStore) PutTicket(_ context.Context, token string, ticket domain.Ticket, _ time.Duration) error {
	if f.forceErr != nil {
		return f.forceErr
	}
	if f.forcePutErr != nil {
		return f.forcePutErr
	}
	f.tickets[token] = ticket
	return nil
}

func (f *fakeStore) GetTicket(_ context.Context, token string) (domain.Ticket, error) {
	if f.forceErr != nil {
		return domain.Ticket{}, f.forceErr
	}
	ticket, ok := f.tickets[token]
	if !ok {
		return domain.Ticket{}, ErrTicketNotFound
	}
	return ticket, nil
}

func TestTicketService_MintThenValidate(t *testing.T) {
	store := newFakeStore()
	svc := NewTicketService(store, 60*time.Second, 10, time.Minute)

	snapshot := domain.Ticket{TenantID: "tenant-a", UserID: "user-1"}
	token, ttl, err := svc.Mint(context.Background(), snapshot)
	require.NoError(t, err)
	assert.NotEmpty(t, token)
	assert.Equal(t, 60*time.Second, ttl)

	got, err := svc.Validate(context.Background(), token)
	require.NoError(t, err)
	assert.Equal(t, snapshot, got)
}

func TestTicketService_MintTwiceProducesDistinctTokens(t *testing.T) {
	store := newFakeStore()
	svc := NewTicketService(store, 60*time.Second, 10, time.Minute)

	snapshot := domain.Ticket{TenantID: "tenant-a", UserID: "user-1"}
	token1, _, err := svc.Mint(context.Background(), snapshot)
	require.NoError(t, err)
	token2, _, err := svc.Mint(context.Background(), snapshot)
	require.NoError(t, err)

	assert.NotEqual(t, token1, token2)
}

func TestTicketService_ValidateIsReusableWithinTTL(t *testing.T) {
	store := newFakeStore()
	svc := NewTicketService(store, 60*time.Second, 10, time.Minute)

	snapshot := domain.Ticket{TenantID: "tenant-a", UserID: "user-1"}
	token, _, err := svc.Mint(context.Background(), snapshot)
	require.NoError(t, err)

	_, err = svc.Validate(context.Background(), token)
	require.NoError(t, err)

	// A second Validate call on the same still-valid ticket must succeed —
	// GetTicket never deletes on read, matching native EventSource reconnect
	// on a brief blip (LLD §5.1).
	_, err = svc.Validate(context.Background(), token)
	require.NoError(t, err)
}

func TestTicketService_ValidateNotFound(t *testing.T) {
	store := newFakeStore()
	svc := NewTicketService(store, 60*time.Second, 10, time.Minute)

	_, err := svc.Validate(context.Background(), "unknown-token")
	assert.ErrorIs(t, err, ErrTicketNotFound)
}

func TestTicketService_MintRateLimited(t *testing.T) {
	store := newFakeStore()
	svc := NewTicketService(store, 60*time.Second, 2, time.Minute)

	snapshot := domain.Ticket{TenantID: "tenant-a", UserID: "user-1"}
	_, _, err := svc.Mint(context.Background(), snapshot)
	require.NoError(t, err)
	_, _, err = svc.Mint(context.Background(), snapshot)
	require.NoError(t, err)

	_, _, err = svc.Mint(context.Background(), snapshot)
	assert.ErrorIs(t, err, ErrRateLimited)
}

func TestTicketService_MintFailsClosedOnStoreError(t *testing.T) {
	store := newFakeStore()
	store.forceErr = errors.New("valkey unreachable")
	svc := NewTicketService(store, 60*time.Second, 10, time.Minute)

	_, _, err := svc.Mint(context.Background(), domain.Ticket{TenantID: "tenant-a", UserID: "user-1"})
	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrRateLimited))
}

func TestTicketService_MintFailsClosedOnPutTicketError(t *testing.T) {
	store := newFakeStore()
	store.forcePutErr = errors.New("valkey unreachable")
	svc := NewTicketService(store, 60*time.Second, 10, time.Minute)

	_, _, err := svc.Mint(context.Background(), domain.Ticket{TenantID: "tenant-a", UserID: "user-1"})
	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrRateLimited))
}

func TestTicketService_MintFailsClosedOnTokenGenerationError(t *testing.T) {
	store := newFakeStore()
	svc := NewTicketService(store, 60*time.Second, 10, time.Minute)

	original := randReader
	randReader = brokenReader{}
	defer func() { randReader = original }()

	_, _, err := svc.Mint(context.Background(), domain.Ticket{TenantID: "tenant-a", UserID: "user-1"})
	require.Error(t, err)
}

type brokenReader struct{}

func (brokenReader) Read([]byte) (int, error) {
	return 0, errors.New("entropy source unavailable")
}

func TestTicketService_ValidateFailsClosedOnStoreError(t *testing.T) {
	store := newFakeStore()
	store.forceErr = errors.New("valkey unreachable")
	svc := NewTicketService(store, 60*time.Second, 10, time.Minute)

	_, err := svc.Validate(context.Background(), "any-token")
	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrTicketNotFound))
}
