package valkey

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

// TicketStore implements usecase.Store against real Valkey — the ticket
// key schema and rate-limit counter from LLD §4.
type TicketStore struct {
	client redis.Cmdable
}

func NewTicketStore(client redis.Cmdable) *TicketStore {
	return &TicketStore{client: client}
}

// AllowMint is a fixed-window counter (LLD §4's sse:ticketrate key,
// §9.3's cap): the first increment in a window sets the window's
// expiry, and every increment after that reuses it — never resetting the
// TTL on subsequent calls (a sliding reset would let a caller who mints
// steadily never actually hit the window boundary).
func (s *TicketStore) AllowMint(ctx context.Context, tenantID, userID string, max int, window time.Duration) (bool, error) {
	key := fmt.Sprintf("sse:ticketrate:%s:%s", tenantID, userID)
	count, err := s.client.Incr(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("valkey incr: %w", err)
	}
	if count == 1 {
		if err := s.client.Expire(ctx, key, window).Err(); err != nil {
			return false, fmt.Errorf("valkey expire: %w", err)
		}
	}
	return count <= int64(max), nil
}

func (s *TicketStore) PutTicket(ctx context.Context, token string, ticket domain.Ticket, ttl time.Duration) error {
	data, err := json.Marshal(ticket)
	if err != nil {
		return fmt.Errorf("marshal ticket: %w", err)
	}
	key := fmt.Sprintf("ssegw:ticket:%s", token)
	if err := s.client.Set(ctx, key, data, ttl).Err(); err != nil {
		return fmt.Errorf("valkey set: %w", err)
	}
	return nil
}

// GetTicket never deletes the key it reads — a ticket is reusable within
// its TTL (usecase.TicketService's own doc comment), not single-use.
func (s *TicketStore) GetTicket(ctx context.Context, token string) (domain.Ticket, error) {
	key := fmt.Sprintf("ssegw:ticket:%s", token)
	data, err := s.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return domain.Ticket{}, usecase.ErrTicketNotFound
	}
	if err != nil {
		return domain.Ticket{}, fmt.Errorf("valkey get: %w", err)
	}
	var ticket domain.Ticket
	if err := json.Unmarshal(data, &ticket); err != nil {
		return domain.Ticket{}, fmt.Errorf("unmarshal ticket: %w", err)
	}
	return ticket, nil
}
