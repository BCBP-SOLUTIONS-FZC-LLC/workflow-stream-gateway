package valkey

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

// StreamStore implements usecase.StreamStore — the read side of the
// personal, tenant-wide, and per-department Valkey Streams EventStore
// writes to.
type StreamStore struct {
	client redis.Cmdable
}

func NewStreamStore(client redis.Cmdable) *StreamStore {
	return &StreamStore{client: client}
}

func (s *StreamStore) OldestEntryID(ctx context.Context, tenantID, userID string) (string, bool, error) {
	key := fmt.Sprintf("sse:events:%s:%s", tenantID, userID)
	entries, err := s.client.XRangeN(ctx, key, "-", "+", 1).Result()
	if err != nil {
		return "", false, fmt.Errorf("valkey xrange: %w", err)
	}
	if len(entries) == 0 {
		return "", false, nil
	}
	return entries[0].ID, true, nil
}

func (s *StreamStore) RangeSince(ctx context.Context, tenantID, userID, afterID string) ([]domain.Frame, error) {
	key := fmt.Sprintf("sse:events:%s:%s", tenantID, userID)
	entries, err := s.client.XRange(ctx, key, "("+afterID, "+").Result()
	if err != nil {
		return nil, fmt.Errorf("valkey xrange: %w", err)
	}
	return toFrames(entries), nil
}

// ReadNew issues one multi-key XREAD BLOCK across every given target's
// stream (LLD §6) — cheaper than one round trip per target, and the only
// way to block on N streams at once rather than N separate blocking
// calls. Results come back grouped per-key; this flattens them in the
// order targets were given (personal first, then tenant-wide-or-
// departments) — cross-stream chronological interleaving is not
// guaranteed, consistent with this service's existing no-aggregation
// stance (LLD §7).
func (s *StreamStore) ReadNew(ctx context.Context, targets []usecase.StreamTarget, block time.Duration) ([]domain.Frame, error) {
	streams := make([]string, len(targets)*2)
	for i, t := range targets {
		streams[i] = keyFor(t)
		streams[len(targets)+i] = "$"
	}

	res, err := s.client.XRead(ctx, &redis.XReadArgs{
		Streams: streams,
		Block:   block,
		Count:   100,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("valkey xread: %w", err)
	}

	var frames []domain.Frame
	for _, stream := range res {
		frames = append(frames, toFrames(stream.Messages)...)
	}
	return frames, nil
}

// keyFor derives the Valkey key for one stream target — the only place
// this key-naming scheme is known outside EventStore's own writes.
func keyFor(t usecase.StreamTarget) string {
	switch {
	case t.UserID != "":
		return fmt.Sprintf("sse:events:%s:%s", t.TenantID, t.UserID)
	case t.DepartmentID != "":
		return fmt.Sprintf("sse:events:broadcast:%s:%s", t.TenantID, t.DepartmentID)
	default:
		return fmt.Sprintf("sse:events:broadcast:%s", t.TenantID)
	}
}

func toFrames(entries []redis.XMessage) []domain.Frame {
	frames := make([]domain.Frame, 0, len(entries))
	for _, e := range entries {
		eventType, _ := e.Values["event"].(string)
		var data []byte
		switch v := e.Values["data"].(type) {
		case string:
			data = []byte(v)
		case []byte:
			data = v
		}
		frames = append(frames, domain.Frame{ID: e.ID, EventType: eventType, Data: data})
	}
	return frames
}
