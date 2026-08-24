package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// The 11 wire types this service consumes (LLD §3.1). Payload schemas are
// owned by execution_service's own api/asyncapi.yaml — verified there
// (2026-08-21) that every task-scoped payload carries "assignee_user_ids"
// ([]string), workflow.task.reassigned additionally carries "old_user_id"/
// "new_user_id" (both string), and every instance-scoped payload carries
// "started_by_user_id" (string) and — as of that verification — no
// active-assignees field at all, despite this LLD's §6 routing table
// hedging "+ active assignees, where present": today, "where present" is
// never, for all 5 instance events currently registered.
//
// workflow.instance.started is deliberately NOT in this list (decided
// 2026-08-24, resolving the routing-table contradiction the LLD itself
// had flagged): execution_service's own asyncapi.yaml documents it as
// "not routed to the Dashboard Stream Gateway — the creating client
// already knows synchronously." An unknown type is still accepted and
// ignored with 200 (IsKnownEventType below), so a producer that hasn't
// caught up to this decision never breaks delivery here.
const (
	eventTaskCreated        = "workflow.task.created"
	eventTaskCompleted      = "workflow.task.completed"
	eventTaskDeferred       = "workflow.task.deferred"
	eventTaskReassigned     = "workflow.task.reassigned"
	eventTaskSLAWarning     = "workflow.task.sla-warning"
	eventTaskSLABreached    = "workflow.task.sla-breached"
	eventInstancePaused     = "workflow.instance.paused"
	eventInstanceResumed    = "workflow.instance.resumed"
	eventInstanceCancelled  = "workflow.instance.cancelled"
	eventInstanceTerminated = "workflow.instance.terminated"
	eventInstanceFinished   = "workflow.instance.finished"
)

var taskAssigneeEvents = map[string]bool{
	eventTaskCreated:     true,
	eventTaskCompleted:   true,
	eventTaskDeferred:    true,
	eventTaskSLAWarning:  true,
	eventTaskSLABreached: true,
}

var instanceEvents = map[string]bool{
	eventInstancePaused:     true,
	eventInstanceResumed:    true,
	eventInstanceCancelled:  true,
	eventInstanceTerminated: true,
	eventInstanceFinished:   true,
}

// ErrUnknownEventType is returned for a wire type absent from the 11
// known types — the handler treats this as forward-compatible, not an
// error (LLD §3.2 step 2: log, count, 200).
var ErrUnknownEventType = errors.New("unknown event type")

// ErrInvalidPayload means a known event type's payload didn't match its
// expected shape — a producer-side bug, not a delivery problem, so the
// handler maps this to 400 (non-retryable) rather than 502.
var ErrInvalidPayload = errors.New("event payload does not match expected schema for its type")

// IsKnownEventType reports whether eventType is one of the 11 types this
// service routes (LLD §3.1). The handler checks this before calling
// Ingest so an unknown type never even reaches recipient resolution.
func IsKnownEventType(eventType string) bool {
	return taskAssigneeEvents[eventType] || instanceEvents[eventType] || eventType == eventTaskReassigned
}

type taskAssigneePayload struct {
	AssigneeUserIDs []string `json:"assignee_user_ids"`
}

type reassignedPayload struct {
	OldUserID string `json:"old_user_id"`
	NewUserID string `json:"new_user_id"`
}

type instanceStartedByPayload struct {
	StartedByUserID string `json:"started_by_user_id"`
}

// resolveRecipients extracts the recipient user IDs for a known event
// type from its raw JSON payload (LLD §6). Recipients are returned even
// if some are empty strings — callers skip those rather than treating
// them as an error, since an upstream data-quality issue in a producer's
// payload isn't this service's validation responsibility to police (LLD
// §1: "no aggregation, no enrichment").
func resolveRecipients(eventType string, payload []byte) ([]string, error) {
	switch {
	case eventType == eventTaskReassigned:
		var p reassignedPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidPayload, err)
		}
		return []string{p.OldUserID, p.NewUserID}, nil
	case taskAssigneeEvents[eventType]:
		var p taskAssigneePayload
		if err := json.Unmarshal(payload, &p); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidPayload, err)
		}
		return p.AssigneeUserIDs, nil
	case instanceEvents[eventType]:
		var p instanceStartedByPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidPayload, err)
		}
		return []string{p.StartedByUserID}, nil
	default:
		return nil, ErrUnknownEventType
	}
}

// EventStreamStore is the persistence FanoutService needs — one XADD per
// recipient (LLD §3.2 step 4, §4's sse:events:<tenant_id>:<user_id> key).
type EventStreamStore interface {
	AppendEvent(ctx context.Context, tenantID, userID, eventType string, rawEnvelope []byte) error
}

// IngestInput is what the HTTP handler hands to FanoutService.Ingest.
// Payload is just the envelope's "data" sub-object (used to resolve
// recipients); RawEnvelope is the full original request body, stored
// verbatim as the SSE frame's data field (LLD §7 — no aggregation, no
// re-encoding that could reorder or drop fields).
type IngestInput struct {
	EventType   string
	TenantID    string
	Payload     []byte
	RawEnvelope []byte
}

type FanoutService struct {
	store EventStreamStore
}

func NewFanoutService(store EventStreamStore) *FanoutService {
	return &FanoutService{store: store}
}

// Ingest resolves recipients and writes one stream entry per recipient.
// A single AppendEvent failure aborts immediately (LLD §3.2 step 5) — the
// caller (HTTP handler) maps that to 502, and a retry that re-delivers to
// recipients whose write already succeeded is harmless (LLD §8: duplicate
// delivery is a no-op for an idempotent frontend reducer).
func (s *FanoutService) Ingest(ctx context.Context, in IngestInput) error {
	recipients, err := resolveRecipients(in.EventType, in.Payload)
	if err != nil {
		return err
	}

	for _, userID := range recipients {
		if userID == "" {
			continue
		}
		if err := s.store.AppendEvent(ctx, in.TenantID, userID, in.EventType, in.RawEnvelope); err != nil {
			return fmt.Errorf("append event: %w", err)
		}
	}
	return nil
}
