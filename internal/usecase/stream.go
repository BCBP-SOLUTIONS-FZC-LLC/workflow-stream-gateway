package usecase

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
)

const (
	tenantAdminRole = "tenant_admin"
	tenantOwnerRole = "tenant_owner"
)

type StreamTarget struct {
	TenantID     string
	UserID       string
	DepartmentID string
}

type StreamPosition struct {
	Target  StreamTarget
	AfterID string
}

type StreamStore interface {
	KeyFor(target StreamTarget) string
	OldestEntryID(ctx context.Context, target StreamTarget) (id string, exists bool, err error)
	LatestEntryID(ctx context.Context, target StreamTarget) (string, error)
	RangeSince(ctx context.Context, target StreamTarget, afterID string) ([]domain.Frame, error)
	ReadNew(ctx context.Context, positions []StreamPosition, block time.Duration) ([]domain.Frame, error)
}

type ConnectionLimiter interface {
	TryAcquire(ctx context.Context, tenantID, userID string, cap int, ttl time.Duration) (bool, error)
	Release(ctx context.Context, tenantID, userID string) error
	Refresh(ctx context.Context, tenantID, userID string, ttl time.Duration) error
}

type FrameSink interface {
	WriteFrame(id, eventType string, data []byte) error
	WriteControlFrame(eventType string, data []byte) error
	WriteHeartbeat() error
}

type StreamService struct {
	tickets           *TicketService
	store             StreamStore
	limiter           ConnectionLimiter
	connCap           int
	connTTL           time.Duration
	heartbeatInterval time.Duration
}

func NewStreamService(
	tickets *TicketService,
	store StreamStore,
	limiter ConnectionLimiter,
	connCap int,
	connTTL time.Duration,
	heartbeatInterval time.Duration,
) *StreamService {
	return &StreamService{
		tickets:           tickets,
		store:             store,
		limiter:           limiter,
		connCap:           connCap,
		connTTL:           connTTL,
		heartbeatInterval: heartbeatInterval,
	}
}

func (s *StreamService) ValidateTicket(ctx context.Context, token string) (domain.Ticket, error) {
	return s.tickets.Validate(ctx, token)
}

func (s *StreamService) AcquireConnection(ctx context.Context, tenantID, userID string) (bool, error) {
	return s.limiter.TryAcquire(ctx, tenantID, userID, s.connCap, s.connTTL)
}

func (s *StreamService) ReleaseConnection(ctx context.Context, tenantID, userID string) error {
	return s.limiter.Release(ctx, tenantID, userID)
}

func streamTargets(ticket domain.Ticket) []StreamTarget {
	targets := []StreamTarget{{TenantID: ticket.TenantID, UserID: ticket.UserID}}

	if isTenantWideEligible(ticket.TenantRoles) {
		return append(targets, StreamTarget{TenantID: ticket.TenantID})
	}

	seen := make(map[string]bool, len(ticket.Departments))
	for _, d := range ticket.Departments {
		if d.DepartmentID == "" || seen[d.DepartmentID] {
			continue
		}
		seen[d.DepartmentID] = true
		targets = append(targets, StreamTarget{TenantID: ticket.TenantID, DepartmentID: d.DepartmentID})
	}
	return targets
}

func isTenantWideEligible(roles []string) bool {
	for _, r := range roles {
		if r == tenantAdminRole || r == tenantOwnerRole {
			return true
		}
	}
	return false
}

func (s *StreamService) Serve(ctx context.Context, ticket domain.Ticket, lastEventID string, sink FrameSink) error {
	targets := streamTargets(ticket)

	latest := make(map[string]string, len(targets))
	for _, t := range targets {
		id, err := s.store.LatestEntryID(ctx, t)
		if err != nil {
			return err
		}
		latest[s.store.KeyFor(t)] = id
	}

	incoming := decodeCursor(lastEventID)
	cursors := make(map[string]string, len(targets))
	for _, t := range targets {
		key := s.store.KeyFor(t)
		if id, ok := incoming[key]; ok {
			cursors[key] = id
		}
	}

	frames, resyncRequired, err := s.backfill(ctx, targets, cursors)
	if err != nil {
		return err
	}
	positions := make(map[string]string, len(targets))
	for key, id := range latest {
		positions[key] = id
	}
	if !resyncRequired {
		for key, id := range cursors {
			positions[key] = id
		}
	}
	for _, f := range frames {
		cursors[f.SourceKey] = f.ID
		positions[f.SourceKey] = f.ID
		if err := sink.WriteFrame(encodeCursor(cursors), f.EventType, f.Data); err != nil {
			return err
		}
	}
	if resyncRequired {
		if err := sink.WriteControlFrame("resync-required", []byte(`{"reason":"backfill_window_exceeded"}`)); err != nil {
			return err
		}
	}

	for {
		if ctx.Err() != nil {
			return nil //nolint:nilerr // the client closing the stream ends it normally
		}
		reads := make([]StreamPosition, len(targets))
		for i, t := range targets {
			reads[i] = StreamPosition{Target: t, AfterID: positions[s.store.KeyFor(t)]}
		}
		newFrames, err := s.store.ReadNew(ctx, reads, s.heartbeatInterval)
		if err != nil {
			if ctx.Err() != nil {
				return nil //nolint:nilerr // a read cut short by the client closing the stream ends it normally
			}
			return err
		}
		_ = s.limiter.Refresh(ctx, ticket.TenantID, ticket.UserID, s.connTTL)
		if len(newFrames) == 0 {
			if err := sink.WriteHeartbeat(); err != nil {
				return err
			}
			continue
		}
		for _, f := range newFrames {
			cursors[f.SourceKey] = f.ID
			positions[f.SourceKey] = f.ID
			if err := sink.WriteFrame(encodeCursor(cursors), f.EventType, f.Data); err != nil {
				return err
			}
		}
	}
}

func (s *StreamService) backfill(ctx context.Context, targets []StreamTarget, cursors map[string]string) ([]domain.Frame, bool, error) {
	if len(cursors) == 0 {
		return nil, false, nil
	}

	var frames []domain.Frame
	for _, t := range targets {
		lastID, ok := cursors[s.store.KeyFor(t)]
		if !ok {
			continue
		}

		oldestID, exists, err := s.store.OldestEntryID(ctx, t)
		if err != nil {
			return nil, false, err
		}
		if !exists || compareStreamIDs(oldestID, lastID) > 0 {
			return nil, true, nil
		}

		targetFrames, err := s.store.RangeSince(ctx, t, lastID)
		if err != nil {
			return nil, false, err
		}
		frames = append(frames, targetFrames...)
	}
	return frames, false, nil
}

func compareStreamIDs(a, b string) int {
	aMs, aSeq := parseStreamID(a)
	bMs, bSeq := parseStreamID(b)
	if aMs != bMs {
		if aMs < bMs {
			return -1
		}
		return 1
	}
	if aSeq != bSeq {
		if aSeq < bSeq {
			return -1
		}
		return 1
	}
	return 0
}

func parseStreamID(id string) (ms, seq int64) {
	parts := strings.SplitN(id, "-", 2)
	ms, _ = strconv.ParseInt(parts[0], 10, 64)
	if len(parts) > 1 {
		seq, _ = strconv.ParseInt(parts[1], 10, 64)
	}
	return ms, seq
}
