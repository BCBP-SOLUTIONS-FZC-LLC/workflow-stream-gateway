package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
)

var ErrUnknownEventType = errors.New("unknown event type")

var ErrInvalidPayload = errors.New("event payload does not match expected schema for its type")

var ErrCallerNotAllowed = errors.New("the caller may not publish for this source")

type CallerCheck bool

const (
	CallerCheckOn  CallerCheck = true
	CallerCheckOff CallerCheck = false
)

type target struct {
	users       []string
	departments []string
}

func resolveTarget(payload []byte) (target, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return target{}, fmt.Errorf("%w: %v", ErrInvalidPayload, err)
	}
	if fields == nil {
		return target{}, fmt.Errorf("%w: the payload must be a JSON object", ErrInvalidPayload)
	}
	raw := fields["recipients"]
	if len(raw) == 0 || string(raw) == "null" {
		return target{}, fmt.Errorf("%w: recipients is required", ErrInvalidPayload)
	}
	var entries []*domain.Recipient
	if err := json.Unmarshal(raw, &entries); err != nil {
		return target{}, fmt.Errorf("%w: recipients must be a list of {kind, id}: %v", ErrInvalidPayload, err)
	}
	var t target
	seen := map[domain.Recipient]bool{}
	for i, entry := range entries {
		if entry == nil {
			return target{}, fmt.Errorf("%w: recipients[%d] must be an object", ErrInvalidPayload, i)
		}
		if err := entry.Validate(); err != nil {
			return target{}, fmt.Errorf("%w: recipients[%d]: %v", ErrInvalidPayload, i, err)
		}
		if seen[*entry] {
			continue
		}
		seen[*entry] = true
		if entry.Kind == domain.RecipientUser {
			t.users = append(t.users, entry.ID)
		} else {
			t.departments = append(t.departments, entry.ID)
		}
	}
	return t, nil
}

type EventStreamStore interface {
	AppendEvent(ctx context.Context, tenantID, userID, eventType string, rawEnvelope []byte) error
	AppendBroadcastEvent(ctx context.Context, tenantID, eventType string, rawEnvelope []byte) error
	AppendDepartmentEvent(ctx context.Context, tenantID, departmentID, eventType string, rawEnvelope []byte) error
}

type IngestInput struct {
	Source      string
	Caller      string
	EventType   string
	TenantID    string
	Payload     []byte
	RawEnvelope []byte
}

type FanoutService struct {
	store       EventStreamStore
	routes      *domain.RoutingTable
	callerCheck CallerCheck
}

func NewFanoutService(store EventStreamStore, routes *domain.RoutingTable, callerCheck CallerCheck) *FanoutService {
	return &FanoutService{store: store, routes: routes, callerCheck: callerCheck}
}

func (s *FanoutService) Ingest(ctx context.Context, in IngestInput) (string, error) {
	route, ok := s.routes.Lookup(in.Source, in.EventType)
	if !ok {
		return "", ErrUnknownEventType
	}
	name := route.StreamEventType()
	if s.callerCheck == CallerCheckOn && !route.Admits(in.Caller) {
		return name, ErrCallerNotAllowed
	}
	t, err := resolveTarget(in.Payload)
	if err != nil {
		return name, err
	}

	for _, userID := range t.users {
		if err := s.store.AppendEvent(ctx, in.TenantID, userID, name, in.RawEnvelope); err != nil {
			return name, fmt.Errorf("append event: %w", err)
		}
	}

	if route.TenantAdmins {
		if err := s.store.AppendBroadcastEvent(ctx, in.TenantID, name, in.RawEnvelope); err != nil {
			return name, fmt.Errorf("append broadcast event: %w", err)
		}
	}

	for _, departmentID := range t.departments {
		if err := s.store.AppendDepartmentEvent(ctx, in.TenantID, departmentID, name, in.RawEnvelope); err != nil {
			return name, fmt.Errorf("append department event: %w", err)
		}
	}

	return name, nil
}
