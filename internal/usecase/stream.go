package usecase

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
)

// StreamStore is the read side of the per-user Valkey Stream the fan-out
// path writes to (LLD §4, §8).
type StreamStore interface {
	// OldestEntryID returns the ID of the oldest entry currently retained,
	// or exists=false if the key is empty or expired.
	OldestEntryID(ctx context.Context, tenantID, userID string) (id string, exists bool, err error)
	// RangeSince returns entries with ID > afterID, oldest first.
	RangeSince(ctx context.Context, tenantID, userID, afterID string) ([]domain.Frame, error)
	// ReadNew blocks up to block for entries newer than "now", returning a
	// nil slice (no error) on timeout — the caller treats that as "send a
	// heartbeat and loop again," not a failure.
	ReadNew(ctx context.Context, tenantID, userID string, block time.Duration) ([]domain.Frame, error)
}

// ConnectionLimiter enforces the per-user concurrent-connection cap (LLD
// §9.3, §4's sse:conncount key).
type ConnectionLimiter interface {
	// TryAcquire increments the counter and reports whether the caller is
	// under cap; if not, the increment is rolled back before returning.
	TryAcquire(ctx context.Context, tenantID, userID string, cap int, ttl time.Duration) (bool, error)
	Release(ctx context.Context, tenantID, userID string) error
}

// FrameSink is how StreamService.Serve emits bytes — implemented by the
// HTTP adapter over the actual response writer, keeping this package free
// of any net/http dependency.
type FrameSink interface {
	WriteFrame(id, eventType string, data []byte) error
	// WriteControlFrame is for a frame with no natural resume-token ID
	// (the resync-required marker, LLD §8) — no "id:" line is written.
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

// ValidateTicket delegates to TicketService — GET /api/v1/stream's
// connection identity comes exclusively from the ticket's stored
// snapshot (LLD §9.1), never a client-supplied value.
func (s *StreamService) ValidateTicket(ctx context.Context, token string) (domain.Ticket, error) {
	return s.tickets.Validate(ctx, token)
}

func (s *StreamService) AcquireConnection(ctx context.Context, tenantID, userID string) (bool, error) {
	return s.limiter.TryAcquire(ctx, tenantID, userID, s.connCap, s.connTTL)
}

func (s *StreamService) ReleaseConnection(ctx context.Context, tenantID, userID string) error {
	return s.limiter.Release(ctx, tenantID, userID)
}

// Serve runs backfill-then-live-tail until ctx is done or an
// unrecoverable store error occurs. A Valkey outage mid-stream closes the
// connection rather than retrying indefinitely — LLD Appendix B leaves
// this choice explicitly undecided; closing is the simpler, safer
// default (an open connection silently retrying forever against a downed
// store is worse than forcing an ordinary client reconnect).
func (s *StreamService) Serve(ctx context.Context, tenantID, userID, lastEventID string, sink FrameSink) error {
	frames, resyncRequired, err := s.backfill(ctx, tenantID, userID, lastEventID)
	if err != nil {
		return err
	}
	for _, f := range frames {
		if err := sink.WriteFrame(f.ID, f.EventType, f.Data); err != nil {
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
			return nil
		}
		newFrames, err := s.store.ReadNew(ctx, tenantID, userID, s.heartbeatInterval)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if len(newFrames) == 0 {
			if err := sink.WriteHeartbeat(); err != nil {
				return err
			}
			continue
		}
		for _, f := range newFrames {
			if err := sink.WriteFrame(f.ID, f.EventType, f.Data); err != nil {
				return err
			}
		}
	}
}

// backfill returns the frames to replay for a reconnecting client (LLD
// §8). A fresh connection (no lastEventID) replays nothing. Beyond the
// retention window — the oldest retained entry is already newer than
// lastEventID, or the stream key itself expired — no partial replay is
// attempted; the caller emits a resync-required control frame instead.
func (s *StreamService) backfill(ctx context.Context, tenantID, userID, lastEventID string) ([]domain.Frame, bool, error) {
	if lastEventID == "" {
		return nil, false, nil
	}

	oldestID, exists, err := s.store.OldestEntryID(ctx, tenantID, userID)
	if err != nil {
		return nil, false, err
	}
	if !exists || compareStreamIDs(oldestID, lastEventID) > 0 {
		return nil, true, nil
	}

	frames, err := s.store.RangeSince(ctx, tenantID, userID, lastEventID)
	if err != nil {
		return nil, false, err
	}
	return frames, false, nil
}

// compareStreamIDs orders two Valkey Stream IDs ("<ms>-<seq>") by their
// numeric value, not lexicographically — a plain string comparison would
// misorder IDs of differing digit-count once one operand isn't a
// well-formed recent timestamp (e.g. a garbage client-supplied
// last_event_id). An unparseable segment is treated as 0, which pushes a
// malformed ID toward "very old" — the safe direction, since backfill()
// then naturally routes it to resync-required rather than a bogus range.
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
