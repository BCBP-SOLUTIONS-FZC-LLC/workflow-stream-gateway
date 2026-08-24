package usecase

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
)

// The exact role names execution_service's own adminRoles map gates on
// (internal/adapter/inbound/http/handler/handler.go:20-24) — this
// platform has no platform-level "admin" role; x-tenant-roles is fully
// open-ended, so reusing a bare "admin" string here would invent a third,
// inconsistent convention.
const (
	tenantAdminRole = "tenant_admin"
	tenantOwnerRole = "tenant_owner"
)

// StreamTarget names one Valkey Stream a connection should live-tail (LLD
// §6's two-tier broadcast model). The adapter decides the actual Valkey
// key from these fields — this usecase layer knows only that a
// connection may need to read its personal stream, the tenant-wide
// broadcast stream, and/or one or more per-department broadcast streams
// simultaneously, never the key-naming scheme itself.
type StreamTarget struct {
	TenantID string
	// UserID is set only for the personal target.
	UserID string
	// DepartmentID is set only for a department-broadcast target.
	DepartmentID string
}

// StreamStore is the read side of the Valkey Streams the fan-out path
// writes to (LLD §4, §6, §8).
type StreamStore interface {
	// KeyFor returns the identifier a target's stream is tagged with —
	// the same value OldestEntryID/RangeSince/ReadNew's returned Frames
	// carry as Frame.SourceKey, and the compound resume cursor's (LLD
	// §8) per-target key. The adapter, not this package, owns the actual
	// Valkey key-naming scheme — this just exposes it.
	KeyFor(target StreamTarget) string
	// OldestEntryID returns the ID of the oldest entry currently
	// retained in target's stream, or exists=false if the key is empty
	// or expired.
	OldestEntryID(ctx context.Context, target StreamTarget) (id string, exists bool, err error)
	// RangeSince returns target's stream entries with ID > afterID,
	// oldest first, each tagged with target's own key (Frame.SourceKey).
	RangeSince(ctx context.Context, target StreamTarget, afterID string) ([]domain.Frame, error)
	// ReadNew blocks up to block for entries newer than "now" across
	// every given target's stream, returning a nil slice (no error) on
	// timeout — the caller treats that as "send a heartbeat and loop
	// again," not a failure.
	ReadNew(ctx context.Context, targets []StreamTarget, block time.Duration) ([]domain.Frame, error)
}

// ConnectionLimiter enforces the per-user concurrent-connection cap (LLD
// §9.3, §4's sse:conncount key) — always per (tenant_id, user_id)
// regardless of how many streams that connection ends up live-tailing.
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

// streamTargets computes the full set of Streams this ticket's connection
// should live-tail (LLD §6): always the personal stream; the tenant-wide
// broadcast stream if the ticket's roles qualify (which makes every
// department-specific target redundant — the tenant-wide stream is
// already a superset — so those are skipped entirely); otherwise one
// target per distinct department the ticket holds any role in, "any
// role" deliberately — every role gets a complete feed for its own
// scope, not just a privileged subset.
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

// Serve runs backfill-then-live-tail until ctx is done or an
// unrecoverable store error occurs. A Valkey outage mid-stream closes the
// connection rather than retrying indefinitely — LLD Appendix B leaves
// this choice explicitly undecided; closing is the simpler, safer
// default (an open connection silently retrying forever against a downed
// store is worse than forcing an ordinary client reconnect).
func (s *StreamService) Serve(ctx context.Context, ticket domain.Ticket, lastEventID string, sink FrameSink) error {
	targets := streamTargets(ticket)

	// cursors seeds only the keys that both (a) the incoming compound
	// Last-Event-ID names and (b) this connection is actually tailing
	// right now — a stale key from a department the ticket no longer
	// holds is dropped, never carried forward (LLD §8, §9).
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
	for _, f := range frames {
		cursors[f.SourceKey] = f.ID
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
			return nil
		}
		newFrames, err := s.store.ReadNew(ctx, targets, s.heartbeatInterval)
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
			cursors[f.SourceKey] = f.ID
			// Every frame's "id:" is the full cursor snapshot after this
			// frame, not just this frame's own stream ID — a single
			// EventSource Last-Event-ID has to carry every tailed
			// stream's position for the next reconnect (LLD §8).
			if err := sink.WriteFrame(encodeCursor(cursors), f.EventType, f.Data); err != nil {
				return err
			}
		}
	}
}

// backfill returns, per target, the frames to replay for a reconnecting
// client (LLD §8). cursors is already scoped to targets this connection
// is tailing (Serve's own seeding step) — a target absent from cursors
// (fresh connection, or a target the ticket only just gained) gets no
// backfill, matching the original single-stream design's "no
// lastEventID -> replay nothing," just decided per-target instead of
// connection-wide. If ANY target's retention window has been exceeded —
// its oldest retained entry is already newer than that target's cursor,
// or its stream key itself expired — the WHOLE connection gets
// resync-required rather than a partial replay: a client silently
// missing events on some streams while others replay cleanly is worse
// than its existing full-REST-refetch fallback.
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
