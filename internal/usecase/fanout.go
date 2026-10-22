package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

const (
	eventTaskCreated        = "WorkflowTaskCreated"
	eventTaskCompleted      = "WorkflowTaskCompleted"
	eventTaskDeferred       = "WorkflowTaskDeferred"
	eventTaskReassigned     = "WorkflowTaskReassigned"
	eventTaskSLAWarning     = "WorkflowTaskSlaWarning"
	eventTaskSLABreached    = "WorkflowTaskSlaBreached"
	eventInstancePaused     = "WorkflowInstancePaused"
	eventInstanceResumed    = "WorkflowInstanceResumed"
	eventInstanceCancelled  = "WorkflowInstanceCancelled"
	eventInstanceTerminated = "WorkflowInstanceTerminated"
	eventInstanceFinished   = "WorkflowInstanceFinished"
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

func KnownEventTypes() []string {
	return []string{
		eventTaskCreated, eventTaskCompleted, eventTaskDeferred, eventTaskReassigned, eventTaskSLAWarning, eventTaskSLABreached,
		eventInstancePaused, eventInstanceResumed, eventInstanceCancelled, eventInstanceTerminated, eventInstanceFinished,
	}
}

func isTaskScoped(eventType string) bool {
	return taskAssigneeEvents[eventType] || eventType == eventTaskReassigned
}

var ErrUnknownEventType = errors.New("unknown event type")

var ErrInvalidPayload = errors.New("event payload does not match expected schema for its type")

func IsKnownEventType(eventType string) bool {
	return taskAssigneeEvents[eventType] || instanceEvents[eventType] || eventType == eventTaskReassigned
}

type taskAssigneePayload struct {
	AssigneeUserIDs []string `json:"assignee_user_ids"`
	DepartmentID    string   `json:"department_id"`
}

type reassignedPayload struct {
	OldUserID    string `json:"old_user_id"`
	NewUserID    string `json:"new_user_id"`
	DepartmentID string `json:"department_id"`
}

type instanceStartedByPayload struct {
	StartedByUserID string `json:"started_by_user_id"`
}

type target struct {
	recipients   []string
	departmentID string
}

func resolveTarget(eventType string, payload []byte) (target, error) {
	switch {
	case eventType == eventTaskReassigned:
		var p reassignedPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			return target{}, fmt.Errorf("%w: %v", ErrInvalidPayload, err)
		}
		return target{recipients: []string{p.OldUserID, p.NewUserID}, departmentID: p.DepartmentID}, nil
	case taskAssigneeEvents[eventType]:
		var p taskAssigneePayload
		if err := json.Unmarshal(payload, &p); err != nil {
			return target{}, fmt.Errorf("%w: %v", ErrInvalidPayload, err)
		}
		return target{recipients: p.AssigneeUserIDs, departmentID: p.DepartmentID}, nil
	case instanceEvents[eventType]:
		var p instanceStartedByPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			return target{}, fmt.Errorf("%w: %v", ErrInvalidPayload, err)
		}
		return target{recipients: []string{p.StartedByUserID}}, nil
	default:
		return target{}, ErrUnknownEventType
	}
}

type EventStreamStore interface {
	AppendEvent(ctx context.Context, tenantID, userID, eventType string, rawEnvelope []byte) error
	AppendBroadcastEvent(ctx context.Context, tenantID, eventType string, rawEnvelope []byte) error
	AppendDepartmentEvent(ctx context.Context, tenantID, departmentID, eventType string, rawEnvelope []byte) error
}

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

func (s *FanoutService) Ingest(ctx context.Context, in IngestInput) error {
	t, err := resolveTarget(in.EventType, in.Payload)
	if err != nil {
		return err
	}

	for _, userID := range t.recipients {
		if userID == "" {
			continue
		}
		if err := s.store.AppendEvent(ctx, in.TenantID, userID, in.EventType, in.RawEnvelope); err != nil {
			return fmt.Errorf("append event: %w", err)
		}
	}

	if err := s.store.AppendBroadcastEvent(ctx, in.TenantID, in.EventType, in.RawEnvelope); err != nil {
		return fmt.Errorf("append broadcast event: %w", err)
	}

	if isTaskScoped(in.EventType) && t.departmentID != "" {
		if err := s.store.AppendDepartmentEvent(ctx, in.TenantID, t.departmentID, in.EventType, in.RawEnvelope); err != nil {
			return fmt.Errorf("append department event: %w", err)
		}
	}

	return nil
}
