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

type target struct {
	recipients   []string
	departmentID string
}

func resolveTarget(route domain.Route, payload []byte) (target, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return target{}, fmt.Errorf("%w: %v", ErrInvalidPayload, err)
	}
	var t target
	for _, name := range route.Recipients {
		ids, err := userIDs(fields[name])
		if err != nil {
			return target{}, fmt.Errorf("%w: field %s: %v", ErrInvalidPayload, name, err)
		}
		t.recipients = append(t.recipients, ids...)
	}
	if route.Department != "" {
		dept, err := optionalString(fields[route.Department])
		if err != nil {
			return target{}, fmt.Errorf("%w: field %s: %v", ErrInvalidPayload, route.Department, err)
		}
		t.departmentID = dept
	}
	return t, nil
}

func userIDs(raw json.RawMessage) ([]string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var one string
	if err := json.Unmarshal(raw, &one); err == nil {
		return []string{one}, nil
	}
	var many []string
	if err := json.Unmarshal(raw, &many); err != nil {
		return nil, errors.New("must be a user ID or an array of user IDs")
	}
	return many, nil
}

func optionalString(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", errors.New("must be a string")
	}
	return s, nil
}

type EventStreamStore interface {
	AppendEvent(ctx context.Context, tenantID, userID, eventType string, rawEnvelope []byte) error
	AppendBroadcastEvent(ctx context.Context, tenantID, eventType string, rawEnvelope []byte) error
	AppendDepartmentEvent(ctx context.Context, tenantID, departmentID, eventType string, rawEnvelope []byte) error
}

type IngestInput struct {
	Source      string
	EventType   string
	TenantID    string
	Payload     []byte
	RawEnvelope []byte
}

type FanoutService struct {
	store  EventStreamStore
	routes *domain.RoutingTable
}

func NewFanoutService(store EventStreamStore, routes *domain.RoutingTable) *FanoutService {
	return &FanoutService{store: store, routes: routes}
}

func (s *FanoutService) Ingest(ctx context.Context, in IngestInput) (string, error) {
	route, ok := s.routes.Lookup(in.Source, in.EventType)
	if !ok {
		return "", ErrUnknownEventType
	}
	name := route.StreamEventType()
	t, err := resolveTarget(route, in.Payload)
	if err != nil {
		return name, err
	}

	for _, userID := range t.recipients {
		if userID == "" {
			continue
		}
		if err := s.store.AppendEvent(ctx, in.TenantID, userID, name, in.RawEnvelope); err != nil {
			return name, fmt.Errorf("append event: %w", err)
		}
	}

	if err := s.store.AppendBroadcastEvent(ctx, in.TenantID, name, in.RawEnvelope); err != nil {
		return name, fmt.Errorf("append broadcast event: %w", err)
	}

	if t.departmentID != "" {
		if err := s.store.AppendDepartmentEvent(ctx, in.TenantID, t.departmentID, name, in.RawEnvelope); err != nil {
			return name, fmt.Errorf("append department event: %w", err)
		}
	}

	return name, nil
}
