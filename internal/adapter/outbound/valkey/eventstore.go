package valkey

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability"
)

type EventStore struct {
	client          redis.Cmdable
	retentionWindow time.Duration
	keyTTL          time.Duration
}

func NewEventStore(client redis.Cmdable, retentionWindow, keyTTL time.Duration) *EventStore {
	return &EventStore{client: client, retentionWindow: retentionWindow, keyTTL: keyTTL}
}

func (s *EventStore) AppendEvent(ctx context.Context, tenantID, userID, eventType string, rawEnvelope []byte) error {
	return s.appendToKey(ctx, fmt.Sprintf("sse:events:%s:%s", tenantID, userID), eventType, rawEnvelope)
}

func (s *EventStore) AppendBroadcastEvent(ctx context.Context, tenantID, eventType string, rawEnvelope []byte) error {
	return s.appendToKey(ctx, fmt.Sprintf("sse:events:broadcast:%s", tenantID), eventType, rawEnvelope)
}

func (s *EventStore) AppendDepartmentEvent(ctx context.Context, tenantID, departmentID, eventType string, rawEnvelope []byte) error {
	return s.appendToKey(ctx, fmt.Sprintf("sse:events:broadcast:%s:%s", tenantID, departmentID), eventType, rawEnvelope)
}

func (s *EventStore) appendToKey(ctx context.Context, key, eventType string, rawEnvelope []byte) error {
	minID := fmt.Sprintf("%d-0", time.Now().Add(-s.retentionWindow).UnixMilli())

	if err := s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: key,
		MinID:  minID,
		Approx: true,
		Values: map[string]any{
			"event": eventType,
			"data":  rawEnvelope,
		},
	}).Err(); err != nil {
		return fmt.Errorf("valkey xadd: %w", err)
	}

	if err := s.client.Expire(ctx, key, s.keyTTL).Err(); err != nil {
		return fmt.Errorf("valkey expire: %w", err)
	}

	if observability.FramesWrittenTotal != nil {
		observability.FramesWrittenTotal.WithLabelValues(eventType).Inc()
	}
	return nil
}
