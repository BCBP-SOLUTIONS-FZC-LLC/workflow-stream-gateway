package usecase

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeEventStore tracks all three append kinds through one shared call
// counter, so failOn can target any specific call regardless of which
// kind it is — matching Ingest's real call order (personal recipients,
// then tenant-wide broadcast, then department broadcast).
type fakeEventStore struct {
	appended   []appendedEvent
	broadcast  []broadcastEvent
	department []departmentEvent
	failOn     int // 1-indexed call number to fail on; 0 means never fail
	calls      int
}

type appendedEvent struct {
	tenantID, userID, eventType string
	rawEnvelope                 []byte
}

type broadcastEvent struct {
	tenantID, eventType string
	rawEnvelope         []byte
}

type departmentEvent struct {
	tenantID, departmentID, eventType string
	rawEnvelope                       []byte
}

func (f *fakeEventStore) AppendEvent(_ context.Context, tenantID, userID, eventType string, rawEnvelope []byte) error {
	f.calls++
	if f.failOn != 0 && f.calls == f.failOn {
		return errors.New("valkey unreachable")
	}
	f.appended = append(f.appended, appendedEvent{tenantID, userID, eventType, rawEnvelope})
	return nil
}

func (f *fakeEventStore) AppendBroadcastEvent(_ context.Context, tenantID, eventType string, rawEnvelope []byte) error {
	f.calls++
	if f.failOn != 0 && f.calls == f.failOn {
		return errors.New("valkey unreachable")
	}
	f.broadcast = append(f.broadcast, broadcastEvent{tenantID, eventType, rawEnvelope})
	return nil
}

func (f *fakeEventStore) AppendDepartmentEvent(_ context.Context, tenantID, departmentID, eventType string, rawEnvelope []byte) error {
	f.calls++
	if f.failOn != 0 && f.calls == f.failOn {
		return errors.New("valkey unreachable")
	}
	f.department = append(f.department, departmentEvent{tenantID, departmentID, eventType, rawEnvelope})
	return nil
}

func TestIsKnownEventType(t *testing.T) {
	known := []string{
		eventTaskCreated, eventTaskCompleted, eventTaskDeferred, eventTaskReassigned,
		eventTaskSLAWarning, eventTaskSLABreached, eventInstancePaused,
		eventInstanceResumed, eventInstanceCancelled, eventInstanceTerminated, eventInstanceFinished,
	}
	for _, eventType := range known {
		assert.True(t, IsKnownEventType(eventType), eventType)
	}
	assert.False(t, IsKnownEventType("user.deleted"))
	assert.False(t, IsKnownEventType(""))
	// Decided 2026-08-24: not consumed, matches execution_service's own
	// asyncapi.yaml ("the creating client already knows synchronously").
	assert.False(t, IsKnownEventType("workflow.instance.started"))
}

func TestFanoutService_Ingest_TaskEventFansOutToAllAssignees(t *testing.T) {
	store := &fakeEventStore{}
	svc := NewFanoutService(store)

	err := svc.Ingest(context.Background(), IngestInput{
		EventType:   eventTaskCreated,
		TenantID:    "tenant-a",
		Payload:     []byte(`{"assignee_user_ids":["u1","u2","u3"],"department_id":"dept-a"}`),
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

func TestFanoutService_Ingest_TaskEventAlwaysBroadcastsTenantWide(t *testing.T) {
	store := &fakeEventStore{}
	svc := NewFanoutService(store)

	err := svc.Ingest(context.Background(), IngestInput{
		EventType: eventTaskCreated,
		TenantID:  "tenant-a",
		Payload:   []byte(`{"assignee_user_ids":["u1"],"department_id":"dept-a"}`),
	})

	require.NoError(t, err)
	require.Len(t, store.broadcast, 1)
	assert.Equal(t, "tenant-a", store.broadcast[0].tenantID)
	assert.Equal(t, eventTaskCreated, store.broadcast[0].eventType)
}

func TestFanoutService_Ingest_TaskEventWithDepartmentIDBroadcastsToDepartment(t *testing.T) {
	store := &fakeEventStore{}
	svc := NewFanoutService(store)

	err := svc.Ingest(context.Background(), IngestInput{
		EventType: eventTaskCreated,
		TenantID:  "tenant-a",
		Payload:   []byte(`{"assignee_user_ids":["u1"],"department_id":"dept-a"}`),
	})

	require.NoError(t, err)
	require.Len(t, store.department, 1)
	assert.Equal(t, "tenant-a", store.department[0].tenantID)
	assert.Equal(t, "dept-a", store.department[0].departmentID)
	assert.Equal(t, eventTaskCreated, store.department[0].eventType)
}

func TestFanoutService_Ingest_TaskEventWithoutDepartmentIDSkipsDepartmentWrite(t *testing.T) {
	store := &fakeEventStore{}
	svc := NewFanoutService(store)

	err := svc.Ingest(context.Background(), IngestInput{
		EventType: eventTaskCreated,
		TenantID:  "tenant-a",
		Payload:   []byte(`{"assignee_user_ids":["u1"]}`),
	})

	require.NoError(t, err)
	assert.Empty(t, store.department)
}

func TestFanoutService_Ingest_ReassignedFansOutToExactlyOldAndNewAndDepartment(t *testing.T) {
	store := &fakeEventStore{}
	svc := NewFanoutService(store)

	err := svc.Ingest(context.Background(), IngestInput{
		EventType:   eventTaskReassigned,
		TenantID:    "tenant-a",
		Payload:     []byte(`{"old_user_id":"old-1","new_user_id":"new-1","department_id":"dept-a"}`),
		RawEnvelope: []byte(`{}`),
	})

	require.NoError(t, err)
	require.Len(t, store.appended, 2)
	assert.Equal(t, "old-1", store.appended[0].userID)
	assert.Equal(t, "new-1", store.appended[1].userID)
	require.Len(t, store.broadcast, 1)
	require.Len(t, store.department, 1)
	assert.Equal(t, "dept-a", store.department[0].departmentID)
}

func TestFanoutService_Ingest_InstanceEventRoutesToStartedByAndBroadcastsButNeverDepartment(t *testing.T) {
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
	require.Len(t, store.broadcast, 1)
	// Instance-scoped payloads carry no department_id at all — never
	// eligible for the department broadcast tier (LLD §6).
	assert.Empty(t, store.department)
}

func TestFanoutService_Ingest_SkipsEmptyRecipients(t *testing.T) {
	store := &fakeEventStore{}
	svc := NewFanoutService(store)

	err := svc.Ingest(context.Background(), IngestInput{
		EventType:   eventInstanceFinished,
		TenantID:    "tenant-a",
		Payload:     []byte(`{"started_by_user_id":""}`),
		RawEnvelope: []byte(`{}`),
	})

	require.NoError(t, err)
	assert.Empty(t, store.appended)
	// The tenant-wide broadcast still happens even with no personal
	// recipient at all.
	assert.Len(t, store.broadcast, 1)
}

func TestFanoutService_Ingest_UnknownEventType(t *testing.T) {
	store := &fakeEventStore{}
	svc := NewFanoutService(store)

	err := svc.Ingest(context.Background(), IngestInput{EventType: "user.deleted", Payload: []byte(`{}`)})

	assert.ErrorIs(t, err, ErrUnknownEventType)
	assert.Empty(t, store.appended)
	assert.Empty(t, store.broadcast)
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

func TestFanoutService_Ingest_AbortsOnFirstPersonalWriteFailure(t *testing.T) {
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
	// u1 succeeded before the failure on the 2nd call (u2); u3 and the
	// broadcast/department writes never attempted.
	require.Len(t, store.appended, 1)
	assert.Equal(t, "u1", store.appended[0].userID)
	assert.Empty(t, store.broadcast)
	assert.Empty(t, store.department)
}

func TestFanoutService_Ingest_AbortsOnBroadcastWriteFailure(t *testing.T) {
	// One assignee (call 1), then the broadcast write (call 2) fails —
	// the department write (which would be call 3) never happens.
	store := &fakeEventStore{failOn: 2}
	svc := NewFanoutService(store)

	err := svc.Ingest(context.Background(), IngestInput{
		EventType: eventTaskCreated,
		TenantID:  "tenant-a",
		Payload:   []byte(`{"assignee_user_ids":["u1"],"department_id":"dept-a"}`),
	})

	require.Error(t, err)
	require.Len(t, store.appended, 1)
	assert.Empty(t, store.broadcast)
	assert.Empty(t, store.department)
}

func TestFanoutService_Ingest_AbortsOnDepartmentWriteFailure(t *testing.T) {
	// One assignee (call 1), broadcast (call 2) both succeed; the
	// department write (call 3) fails.
	store := &fakeEventStore{failOn: 3}
	svc := NewFanoutService(store)

	err := svc.Ingest(context.Background(), IngestInput{
		EventType: eventTaskCreated,
		TenantID:  "tenant-a",
		Payload:   []byte(`{"assignee_user_ids":["u1"],"department_id":"dept-a"}`),
	})

	require.Error(t, err)
	require.Len(t, store.appended, 1)
	require.Len(t, store.broadcast, 1)
	assert.Empty(t, store.department)
}
