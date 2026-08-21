package usecase

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeEventStore struct {
	appended []appendedEvent
	failOn   int // 1-indexed call number to fail on; 0 means never fail
	calls    int
}

type appendedEvent struct {
	tenantID, userID, eventType string
	rawEnvelope                 []byte
}

func (f *fakeEventStore) AppendEvent(_ context.Context, tenantID, userID, eventType string, rawEnvelope []byte) error {
	f.calls++
	if f.failOn != 0 && f.calls == f.failOn {
		return errors.New("valkey unreachable")
	}
	f.appended = append(f.appended, appendedEvent{tenantID, userID, eventType, rawEnvelope})
	return nil
}

func TestIsKnownEventType(t *testing.T) {
	known := []string{
		eventTaskCreated, eventTaskCompleted, eventTaskDeferred, eventTaskReassigned,
		eventTaskSLAWarning, eventTaskSLABreached, eventInstanceStarted, eventInstancePaused,
		eventInstanceResumed, eventInstanceCancelled, eventInstanceTerminated, eventInstanceFinished,
	}
	for _, eventType := range known {
		assert.True(t, IsKnownEventType(eventType), eventType)
	}
	assert.False(t, IsKnownEventType("user.deleted"))
	assert.False(t, IsKnownEventType(""))
}

func TestFanoutService_Ingest_TaskEventFansOutToAllAssignees(t *testing.T) {
	store := &fakeEventStore{}
	svc := NewFanoutService(store)

	err := svc.Ingest(context.Background(), IngestInput{
		EventType:   eventTaskCreated,
		TenantID:    "tenant-a",
		Payload:     []byte(`{"assignee_user_ids":["u1","u2","u3"]}`),
		RawEnvelope: []byte(`{"type":"workflow.task.created"}`),
	})

	require.NoError(t, err)
	require.Len(t, store.appended, 3)
	for i, userID := range []string{"u1", "u2", "u3"} {
		assert.Equal(t, "tenant-a", store.appended[i].tenantID)
		assert.Equal(t, userID, store.appended[i].userID)
		assert.Equal(t, eventTaskCreated, store.appended[i].eventType)
	}
}

func TestFanoutService_Ingest_ReassignedFansOutToExactlyOldAndNew(t *testing.T) {
	store := &fakeEventStore{}
	svc := NewFanoutService(store)

	err := svc.Ingest(context.Background(), IngestInput{
		EventType:   eventTaskReassigned,
		TenantID:    "tenant-a",
		Payload:     []byte(`{"old_user_id":"old-1","new_user_id":"new-1"}`),
		RawEnvelope: []byte(`{}`),
	})

	require.NoError(t, err)
	require.Len(t, store.appended, 2)
	assert.Equal(t, "old-1", store.appended[0].userID)
	assert.Equal(t, "new-1", store.appended[1].userID)
}

func TestFanoutService_Ingest_InstanceEventRoutesToStartedBy(t *testing.T) {
	store := &fakeEventStore{}
	svc := NewFanoutService(store)

	err := svc.Ingest(context.Background(), IngestInput{
		EventType:   eventInstanceFinished,
		TenantID:    "tenant-a",
		Payload:     []byte(`{"started_by_user_id":"starter-1"}`),
		RawEnvelope: []byte(`{}`),
	})

	require.NoError(t, err)
	require.Len(t, store.appended, 1)
	assert.Equal(t, "starter-1", store.appended[0].userID)
}

func TestFanoutService_Ingest_SkipsEmptyRecipients(t *testing.T) {
	store := &fakeEventStore{}
	svc := NewFanoutService(store)

	err := svc.Ingest(context.Background(), IngestInput{
		EventType:   eventInstanceStarted,
		TenantID:    "tenant-a",
		Payload:     []byte(`{"started_by_user_id":""}`),
		RawEnvelope: []byte(`{}`),
	})

	require.NoError(t, err)
	assert.Empty(t, store.appended)
}

func TestFanoutService_Ingest_UnknownEventType(t *testing.T) {
	store := &fakeEventStore{}
	svc := NewFanoutService(store)

	err := svc.Ingest(context.Background(), IngestInput{EventType: "user.deleted", Payload: []byte(`{}`)})

	assert.ErrorIs(t, err, ErrUnknownEventType)
	assert.Empty(t, store.appended)
}

func TestFanoutService_Ingest_InvalidPayloadForKnownType(t *testing.T) {
	store := &fakeEventStore{}
	svc := NewFanoutService(store)

	err := svc.Ingest(context.Background(), IngestInput{
		EventType: eventTaskCreated,
		Payload:   []byte(`{"assignee_user_ids": "not-an-array"}`),
	})

	assert.ErrorIs(t, err, ErrInvalidPayload)
}

func TestFanoutService_Ingest_AbortsOnFirstStoreFailure(t *testing.T) {
	store := &fakeEventStore{failOn: 2}
	svc := NewFanoutService(store)

	err := svc.Ingest(context.Background(), IngestInput{
		EventType: eventTaskCreated,
		TenantID:  "tenant-a",
		Payload:   []byte(`{"assignee_user_ids":["u1","u2","u3"]}`),
	})

	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrInvalidPayload))
	assert.False(t, errors.Is(err, ErrUnknownEventType))
	// u1 succeeded before the failure on the 2nd call (u2); u3 never attempted.
	require.Len(t, store.appended, 1)
	assert.Equal(t, "u1", store.appended[0].userID)
}
