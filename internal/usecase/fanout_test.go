package usecase

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
)

const workflowSource = "workflow-execution-svc"

const llmSource = "llm-retrieval-generation"

func users(ids ...string) string {
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, `{"kind":"user","id":"`+id+`"}`)
	}
	return strings.Join(parts, ",")
}

func dept(id string) string {
	return `{"kind":"department","id":"` + id + `"}`
}

func recipients(entries ...string) []byte {
	return []byte(`{"recipients":[` + strings.Join(entries, ",") + `]}`)
}

const (
	eventTaskCreated      = "WorkflowTaskCreated"
	eventTaskReassigned   = "WorkflowTaskReassigned"
	eventInstanceFinished = "WorkflowInstanceFinished"
)

// workflowTable builds the routing table from the shipped config/routing.yaml,
// so every fan-out test also proves the default file reproduces the routing of
// the Execution Service's events.
func workflowTable(t *testing.T) *domain.RoutingTable {
	t.Helper()
	raw, err := os.ReadFile("../../config/routing.yaml")
	require.NoError(t, err)
	var file domain.RoutingFile
	require.NoError(t, yaml.Unmarshal(raw, &file))
	table, err := domain.NewRoutingTable(file)
	require.NoError(t, err)
	return table
}

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

func TestFanoutService_Ingest_TaskEventFansOutToAllAssignees(t *testing.T) {
	store := &fakeEventStore{}
	svc := NewFanoutService(store, workflowTable(t))

	_, err := svc.Ingest(context.Background(), IngestInput{
		Source: workflowSource, EventType: eventTaskCreated,
		TenantID:    "tenant-a",
		Payload:     recipients(users("u1", "u2", "u3"), dept("dept-a")),
		RawEnvelope: []byte(`{"type":"WorkflowTaskCreated"}`),
	})

	require.NoError(t, err)
	require.Len(t, store.appended, 3)
	for i, userID := range []string{"u1", "u2", "u3"} {
		assert.Equal(t, "tenant-a", store.appended[i].tenantID)
		assert.Equal(t, userID, store.appended[i].userID)
		assert.Equal(t, "workflow."+eventTaskCreated, store.appended[i].eventType)
	}
}

func TestFanoutService_Ingest_TaskEventAlwaysBroadcastsTenantWide(t *testing.T) {
	store := &fakeEventStore{}
	svc := NewFanoutService(store, workflowTable(t))

	_, err := svc.Ingest(context.Background(), IngestInput{
		Source: workflowSource, EventType: eventTaskCreated,
		TenantID: "tenant-a",
		Payload:  recipients(users("u1"), dept("dept-a")),
	})

	require.NoError(t, err)
	require.Len(t, store.broadcast, 1)
	assert.Equal(t, "tenant-a", store.broadcast[0].tenantID)
	assert.Equal(t, "workflow."+eventTaskCreated, store.broadcast[0].eventType)
}

func TestFanoutService_Ingest_TaskEventWithDepartmentIDBroadcastsToDepartment(t *testing.T) {
	store := &fakeEventStore{}
	svc := NewFanoutService(store, workflowTable(t))

	_, err := svc.Ingest(context.Background(), IngestInput{
		Source: workflowSource, EventType: eventTaskCreated,
		TenantID: "tenant-a",
		Payload:  recipients(users("u1"), dept("dept-a")),
	})

	require.NoError(t, err)
	require.Len(t, store.department, 1)
	assert.Equal(t, "tenant-a", store.department[0].tenantID)
	assert.Equal(t, "dept-a", store.department[0].departmentID)
	assert.Equal(t, "workflow."+eventTaskCreated, store.department[0].eventType)
}

func TestFanoutService_Ingest_TaskEventWithoutDepartmentIDSkipsDepartmentWrite(t *testing.T) {
	store := &fakeEventStore{}
	svc := NewFanoutService(store, workflowTable(t))

	_, err := svc.Ingest(context.Background(), IngestInput{
		Source: workflowSource, EventType: eventTaskCreated,
		TenantID: "tenant-a",
		Payload:  recipients(users("u1")),
	})

	require.NoError(t, err)
	assert.Empty(t, store.department)
}

func TestFanoutService_Ingest_ReassignedFansOutToExactlyOldAndNewAndDepartment(t *testing.T) {
	store := &fakeEventStore{}
	svc := NewFanoutService(store, workflowTable(t))

	_, err := svc.Ingest(context.Background(), IngestInput{
		Source: workflowSource, EventType: eventTaskReassigned,
		TenantID:    "tenant-a",
		Payload:     recipients(users("old-1", "new-1"), dept("dept-a")),
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
	svc := NewFanoutService(store, workflowTable(t))

	_, err := svc.Ingest(context.Background(), IngestInput{
		Source: workflowSource, EventType: eventInstanceFinished,
		TenantID:    "tenant-a",
		Payload:     recipients(users("starter-1")),
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

func TestFanoutService_Ingest_AnEmptyRecipientListStillReachesTheTenantTier(t *testing.T) {
	store := &fakeEventStore{}
	svc := NewFanoutService(store, workflowTable(t))

	_, err := svc.Ingest(context.Background(), IngestInput{
		Source: workflowSource, EventType: eventInstanceFinished,
		TenantID:    "tenant-a",
		Payload:     []byte(`{"recipients":[]}`),
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
	svc := NewFanoutService(store, workflowTable(t))

	_, err := svc.Ingest(context.Background(), IngestInput{Source: workflowSource, EventType: "user.deleted", Payload: []byte(`{}`)})

	assert.ErrorIs(t, err, ErrUnknownEventType)
	assert.Empty(t, store.appended)
	assert.Empty(t, store.broadcast)
}

func TestFanoutService_Ingest_InvalidPayloadForKnownType(t *testing.T) {
	store := &fakeEventStore{}
	svc := NewFanoutService(store, workflowTable(t))

	_, err := svc.Ingest(context.Background(), IngestInput{
		Source: workflowSource, EventType: eventTaskCreated,
		Payload: []byte(`{"recipients": 7}`),
	})

	assert.ErrorIs(t, err, ErrInvalidPayload)
}

func TestFanoutService_Ingest_AbortsOnFirstPersonalWriteFailure(t *testing.T) {
	store := &fakeEventStore{failOn: 2}
	svc := NewFanoutService(store, workflowTable(t))

	_, err := svc.Ingest(context.Background(), IngestInput{
		Source: workflowSource, EventType: eventTaskCreated,
		TenantID: "tenant-a",
		Payload:  recipients(users("u1", "u2", "u3")),
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
	svc := NewFanoutService(store, workflowTable(t))

	_, err := svc.Ingest(context.Background(), IngestInput{
		Source: workflowSource, EventType: eventTaskCreated,
		TenantID: "tenant-a",
		Payload:  recipients(users("u1"), dept("dept-a")),
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
	svc := NewFanoutService(store, workflowTable(t))

	_, err := svc.Ingest(context.Background(), IngestInput{
		Source: workflowSource, EventType: eventTaskCreated,
		TenantID: "tenant-a",
		Payload:  recipients(users("u1"), dept("dept-a")),
	})

	require.Error(t, err)
	require.Len(t, store.appended, 1)
	require.Len(t, store.broadcast, 1)
	assert.Empty(t, store.department)
}

func TestFanoutService_Ingest_EveryRenderedEventReachesItsRecipients(t *testing.T) {
	task := recipients(users("u1", "u2"), dept("dept-a"))
	instance := recipients(users("starter-1"))
	for _, tc := range []struct {
		eventType      string
		payload        []byte
		wantRecipients []string
		wantDepartment bool
	}{
		{"WorkflowTaskCreated", task, []string{"u1", "u2"}, true},
		{"WorkflowTaskClaimed", task, []string{"u1", "u2"}, true},
		{"WorkflowTaskCompleted", task, []string{"u1", "u2"}, true},
		{"WorkflowTaskDeferred", task, []string{"u1", "u2"}, true},
		{"WorkflowTaskReassigned", recipients(users("old-1", "new-1"), dept("dept-a")), []string{"old-1", "new-1"}, true},
		{"WorkflowTaskReassigned", recipients(users("new-1"), dept("dept-a")), []string{"new-1"}, true},
		{"WorkflowTaskSuperseded", task, []string{"u1", "u2"}, true},
		{"WorkflowTaskFailed", task, []string{"u1", "u2"}, true},
		{"WorkflowTaskSlaWarning", task, []string{"u1", "u2"}, true},
		{"WorkflowTaskSlaBreached", task, []string{"u1", "u2"}, true},
		{"WorkflowInstancePaused", instance, []string{"starter-1"}, false},
		{"WorkflowInstanceResumed", instance, []string{"starter-1"}, false},
		{"WorkflowInstanceCancelled", instance, []string{"starter-1"}, false},
		{"WorkflowInstanceTerminated", instance, []string{"starter-1"}, false},
		{"WorkflowInstanceDegraded", instance, []string{"starter-1"}, false},
		{"WorkflowInstanceFailed", instance, []string{"starter-1"}, false},
		{"WorkflowInstanceFinished", instance, []string{"starter-1"}, false},
	} {
		t.Run(tc.eventType, func(t *testing.T) {
			store := &fakeEventStore{}

			_, err := NewFanoutService(store, workflowTable(t)).Ingest(context.Background(), IngestInput{
				Source: workflowSource, EventType: tc.eventType, TenantID: "tenant-a", Payload: tc.payload, RawEnvelope: []byte(`{}`),
			})

			require.NoError(t, err)
			var got []string
			for _, e := range store.appended {
				assert.Equal(t, "workflow."+tc.eventType, e.eventType)
				got = append(got, e.userID)
			}
			assert.Equal(t, tc.wantRecipients, got)
			assert.Len(t, store.broadcast, 1, "every workflow event is written tenant-wide")
			assert.Equal(t, tc.wantDepartment, len(store.department) == 1)
		})
	}
}

func TestFanoutService_Ingest_EveryLLMEventReachesOnlyItsRecipients(t *testing.T) {
	for _, eventType := range []string{
		"GenerationStarted", "GenerationProgressed", "OutlineReady", "SectionCompleted",
		"GenerationCompleted", "GenerationFailed", "GenerationCancelled",
		"GenerationGapReported", "ArtefactRegenerated", "ExtractionCompleted",
	} {
		t.Run(eventType, func(t *testing.T) {
			store := &fakeEventStore{}

			name, err := NewFanoutService(store, workflowTable(t)).Ingest(context.Background(), IngestInput{
				Source: llmSource, EventType: eventType, TenantID: "tenant-a",
				Payload: recipients(users("u1"), dept("dept-a")), RawEnvelope: []byte(`{}`),
			})

			require.NoError(t, err)
			assert.Equal(t, "llm."+eventType, name)
			require.Len(t, store.appended, 1)
			assert.Equal(t, "u1", store.appended[0].userID)
			require.Len(t, store.department, 1)
			assert.Equal(t, "dept-a", store.department[0].departmentID)
			assert.Empty(t, store.broadcast, "LLM events do not reach the tenant administrators")
		})
	}
}

func TestFanoutService_Ingest_UnrenderedExecutionEventsAreUnknown(t *testing.T) {
	for _, eventType := range []string{"WorkflowInstanceStarted", "WorkflowInstanceForceRouted"} {
		_, err := NewFanoutService(&fakeEventStore{}, workflowTable(t)).Ingest(context.Background(), IngestInput{
			Source: workflowSource, EventType: eventType, TenantID: "tenant-a", Payload: []byte(`{}`), RawEnvelope: []byte(`{}`),
		})
		assert.ErrorIs(t, err, ErrUnknownEventType, eventType)
	}
}
