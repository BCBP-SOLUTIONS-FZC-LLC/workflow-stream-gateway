package valkey

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability"
)

// EventStore implements usecase.EventStreamStore against real Valkey — the
// per-user, tenant-wide, and per-department Stream keys from LLD §4,
// written by the fan-out path and read by GET /api/v1/stream.
type EventStore struct {
	client          redis.Cmdable
	retentionWindow time.Duration
	keyTTL          time.Duration
}

func NewEventStore(client redis.Cmdable, retentionWindow, keyTTL time.Duration) *EventStore {
	return &EventStore{client: client, retentionWindow: retentionWindow, keyTTL: keyTTL}
}

// AppendEvent writes to a specific user's personal stream.
func (s *EventStore) AppendEvent(ctx context.Context, tenantID, userID, eventType string, rawEnvelope []byte) error {
	return s.appendToKey(ctx, fmt.Sprintf("sse:events:%s:%s", tenantID, userID), eventType, rawEnvelope)
}

// AppendBroadcastEvent writes to the tenant-wide broadcast stream (LLD §6
// — eligible for tenant_admin/tenant_owner roles, all known event types).
func (s *EventStore) AppendBroadcastEvent(ctx context.Context, tenantID, eventType string, rawEnvelope []byte) error {
	return s.appendToKey(ctx, fmt.Sprintf("sse:events:broadcast:%s", tenantID), eventType, rawEnvelope)
}

// AppendDepartmentEvent writes to one department's broadcast stream (LLD
// §6 — eligible for any role held in that department, task-scoped event
// types only).
func (s *EventStore) AppendDepartmentEvent(ctx context.Context, tenantID, departmentID, eventType string, rawEnvelope []byte) error {
	return s.appendToKey(ctx, fmt.Sprintf("sse:events:broadcast:%s:%s", tenantID, departmentID), eventType, rawEnvelope)
}

// appendToKey XADDs one entry (MINID-trimmed to the retention window, LLD
// §4/§8) and refreshes the key's own EXPIRE — the key TTL is deliberately
// longer than the retention window so a quiet stream still answers a late
// backfill read within that window (config.Config.validate() enforces
// StreamKeyTTL > StreamRetentionWindow at boot). Shared by all three
// XADD-writing key shapes above — they differ only in key construction.
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
