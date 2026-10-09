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

var randReader io.Reader = rand.Reader

var ErrRateLimited = errors.New("ticket mint rate limit exceeded")

var ErrTicketNotFound = errors.New("ticket not found or expired")

type Store interface {
	AllowMint(ctx context.Context, tenantID, userID string, max int, window time.Duration) (bool, error)
	PutTicket(ctx context.Context, token string, ticket domain.Ticket, ttl time.Duration) error
	GetTicket(ctx context.Context, token string) (domain.Ticket, error)
}

type TicketService struct {
	store      Store
	ttl        time.Duration
	rateMax    int
	rateWindow time.Duration
}

func NewTicketService(store Store, ttl time.Duration, rateMax int, rateWindow time.Duration) *TicketService {
	return &TicketService{store: store, ttl: ttl, rateMax: rateMax, rateWindow: rateWindow}
}

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

func generateToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := io.ReadFull(randReader, buf); err != nil {
		return "", fmt.Errorf("read random bytes: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}
