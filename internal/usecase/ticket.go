package usecase

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
)

// randReader is a package-level indirection over crypto/rand.Reader so
// tests can exercise generateToken's error path (a real crypto/rand read
// failure isn't reproducible portably) without changing production
// behavior.
var randReader io.Reader = rand.Reader

// ErrRateLimited is returned by Mint when the caller has already minted
// TicketService's configured maximum within the current rolling window
// (LLD §9.3, §4's sse:ticketrate key).
var ErrRateLimited = errors.New("ticket mint rate limit exceeded")

// ErrTicketNotFound is returned by Validate when the ticket is missing or
// expired — distinct from a Store error, since the handler maps this to
// 401 (bad/expired ticket) rather than 503 (store unreachable, LLD §9.4).
var ErrTicketNotFound = errors.New("ticket not found or expired")

// Store is the persistence TicketService needs. Any error other than
// ErrTicketNotFound is treated as the store being unreachable — this
// usecase draws no distinction between "Valkey down" and "some other
// Valkey error," since both fail closed identically (LLD §9.4).
type Store interface {
	AllowMint(ctx context.Context, tenantID, userID string, max int, window time.Duration) (bool, error)
	PutTicket(ctx context.Context, token string, ticket domain.Ticket, ttl time.Duration) error
	GetTicket(ctx context.Context, token string) (domain.Ticket, error)
}

// TicketService mints and validates tickets (LLD §5.1). A ticket is
// reusable within its TTL, not single-use: Validate never deletes or
// otherwise consumes the stored ticket, so an ordinary EventSource
// reconnect on a brief blip can reuse the same still-valid ticket (LLD
// §5.1's own rationale for this choice).
type TicketService struct {
	store      Store
	ttl        time.Duration
	rateMax    int
	rateWindow time.Duration
}

func NewTicketService(store Store, ttl time.Duration, rateMax int, rateWindow time.Duration) *TicketService {
	return &TicketService{store: store, ttl: ttl, rateMax: rateMax, rateWindow: rateWindow}
}

// Mint checks the caller's mint-rate budget, then generates and stores a
// fresh opaque ticket for the given identity snapshot. Returns the token
// and its TTL.
func (s *TicketService) Mint(ctx context.Context, snapshot domain.Ticket) (string, time.Duration, error) {
	allowed, err := s.store.AllowMint(ctx, snapshot.TenantID, snapshot.UserID, s.rateMax, s.rateWindow)
	if err != nil {
		return "", 0, fmt.Errorf("ticket mint rate check: %w", err)
	}
	if !allowed {
		return "", 0, ErrRateLimited
	}

	token, err := generateToken()
	if err != nil {
		return "", 0, fmt.Errorf("generate ticket token: %w", err)
	}

	if err := s.store.PutTicket(ctx, token, snapshot, s.ttl); err != nil {
		return "", 0, fmt.Errorf("ticket mint store: %w", err)
	}

	return token, s.ttl, nil
}

// Validate looks up the ticket's stored snapshot. It is the sole source of
// connection identity for GET /api/v1/stream (LLD §9.1) — callers must
// never fall back to a client-supplied tenant_id/user_id on error.
func (s *TicketService) Validate(ctx context.Context, token string) (domain.Ticket, error) {
	ticket, err := s.store.GetTicket(ctx, token)
	if err != nil {
		if errors.Is(err, ErrTicketNotFound) {
			return domain.Ticket{}, err
		}
		return domain.Ticket{}, fmt.Errorf("ticket validate: %w", err)
	}
	return ticket, nil
}

// generateToken produces a 256-bit crypto/rand value, base64url-encoded
// (LLD §5.1) — opaque, unguessable, and unrelated to the envelope UUIDs
// this service otherwise handles.
func generateToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := io.ReadFull(randReader, buf); err != nil {
		return "", fmt.Errorf("read random bytes: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}
