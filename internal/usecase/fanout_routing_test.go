package usecase

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
)

func twoServiceTable(t *testing.T) *domain.RoutingTable {
	t.Helper()
	table, err := domain.NewRoutingTable(domain.RoutingFile{
		Version: 1,
		Services: map[string]domain.ServiceRoutes{
			"workflow": {Source: workflowSource, Events: map[string]domain.EventRoute{
				"Shared": {Recipients: []string{"assignee_user_ids"}, Department: "department_id"},
			}},
			"tender": {Source: "tender-svc", Events: map[string]domain.EventRoute{
				"Shared": {Recipients: []string{"owner_user_id"}},
			}},
		},
	})
	require.NoError(t, err)
	return table
}

func TestFanoutService_Ingest_TheSameTypeUnderTwoServicesRoutesBySource(t *testing.T) {
	store := &fakeEventStore{}
	svc := NewFanoutService(store, twoServiceTable(t))
	payload := []byte(`{"assignee_user_ids":["a1"],"owner_user_id":"o1","department_id":"dept-a"}`)

	name, err := svc.Ingest(context.Background(), IngestInput{Source: "tender-svc", EventType: "Shared", TenantID: "t", Payload: payload})

	require.NoError(t, err)
	assert.Equal(t, "tender.Shared", name)
	require.Len(t, store.appended, 1)
	assert.Equal(t, "o1", store.appended[0].userID, "the tender entry names owner_user_id")
	assert.Equal(t, "tender.Shared", store.appended[0].eventType)
	assert.Equal(t, "tender.Shared", store.broadcast[0].eventType)
	assert.Empty(t, store.department, "the tender entry names no department field")

	store = &fakeEventStore{}
	name, err = NewFanoutService(store, twoServiceTable(t)).Ingest(context.Background(),
		IngestInput{Source: workflowSource, EventType: "Shared", TenantID: "t", Payload: payload})

	require.NoError(t, err)
	assert.Equal(t, "workflow.Shared", name)
	assert.Equal(t, "a1", store.appended[0].userID)
	require.Len(t, store.department, 1)
	assert.Equal(t, "workflow.Shared", store.department[0].eventType)
}

func TestFanoutService_Ingest_ATypeUnderAnotherSourceIsUnknown(t *testing.T) {
	store := &fakeEventStore{}

	name, err := NewFanoutService(store, workflowTable(t)).Ingest(context.Background(),
		IngestInput{Source: "tender-svc", EventType: eventTaskCreated, TenantID: "t", Payload: []byte(`{}`)})

	assert.ErrorIs(t, err, ErrUnknownEventType)
	assert.Empty(t, name)
	assert.Empty(t, store.appended)
	assert.Empty(t, store.broadcast)
}

func TestFanoutService_Ingest_ReadsARecipientFieldAsOneIDOrAnArray(t *testing.T) {
	for name, tc := range map[string]struct {
		payload string
		want    []string
	}{
		"one ID":            {`{"assignee_user_ids":"u1"}`, []string{"u1"}},
		"array":             {`{"assignee_user_ids":["u1","u2"]}`, []string{"u1", "u2"}},
		"null":              {`{"assignee_user_ids":null}`, nil},
		"missing":           {`{}`, nil},
		"empty in an array": {`{"assignee_user_ids":["","u2"]}`, []string{"u2"}},
	} {
		t.Run(name, func(t *testing.T) {
			store := &fakeEventStore{}

			_, err := NewFanoutService(store, workflowTable(t)).Ingest(context.Background(),
				IngestInput{Source: workflowSource, EventType: eventTaskCreated, TenantID: "t", Payload: []byte(tc.payload)})

			require.NoError(t, err)
			var got []string
			for _, e := range store.appended {
				got = append(got, e.userID)
			}
			assert.Equal(t, tc.want, got)
			assert.Len(t, store.broadcast, 1)
		})
	}
}

func TestFanoutService_Ingest_RefusesAPayloadOfTheWrongShape(t *testing.T) {
	for name, payload := range map[string]string{
		"not an object":            `["u1"]`,
		"null":                     `null`,
		"not JSON":                 `{`,
		"recipient is a number":    `{"assignee_user_ids":7}`,
		"recipient array of mixed": `{"assignee_user_ids":["u1",2]}`,
		"department is a number":   `{"assignee_user_ids":["u1"],"department_id":3}`,
	} {
		t.Run(name, func(t *testing.T) {
			store := &fakeEventStore{}

			_, err := NewFanoutService(store, workflowTable(t)).Ingest(context.Background(),
				IngestInput{Source: workflowSource, EventType: eventTaskCreated, TenantID: "t", Payload: []byte(payload)})

			assert.ErrorIs(t, err, ErrInvalidPayload)
			assert.Empty(t, store.appended, "nothing is written for a payload that does not decode")
			assert.Empty(t, store.broadcast)
		})
	}
}

func TestFanoutService_Ingest_AReassignmentWithNoOldAssigneeReachesOnlyTheNewOne(t *testing.T) {
	for name, payload := range map[string]string{
		"missing": `{"new_user_id":"new-1","department_id":"dept-a"}`,
		"null":    `{"old_user_id":null,"new_user_id":"new-1","department_id":"dept-a"}`,
		"empty":   `{"old_user_id":"","new_user_id":"new-1","department_id":"dept-a"}`,
	} {
		t.Run(name, func(t *testing.T) {
			store := &fakeEventStore{}

			_, err := NewFanoutService(store, workflowTable(t)).Ingest(context.Background(),
				IngestInput{Source: workflowSource, EventType: eventTaskReassigned, TenantID: "t", Payload: []byte(payload)})

			require.NoError(t, err)
			require.Len(t, store.appended, 1)
			assert.Equal(t, "new-1", store.appended[0].userID)
			assert.Len(t, store.department, 1)
		})
	}
}
