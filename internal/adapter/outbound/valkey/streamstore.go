package valkey

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
)

// StreamStore implements usecase.StreamStore — the read side of the
// per-user Valkey Stream EventStore writes to.
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

func (s *StreamStore) ReadNew(ctx context.Context, tenantID, userID string, block time.Duration) ([]domain.Frame, error) {
	key := fmt.Sprintf("sse:events:%s:%s", tenantID, userID)
	res, err := s.client.XRead(ctx, &redis.XReadArgs{
		Streams: []string{key, "$"},
		Block:   block,
		Count:   100,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("valkey xread: %w", err)
	}
	if len(res) == 0 {
		return nil, nil
	}
	return toFrames(res[0].Messages), nil
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
