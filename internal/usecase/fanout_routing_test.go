package usecase

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
)

func twoServiceTable(t *testing.T) *domain.RoutingTable {
	t.Helper()
	yes, no := true, false
	table, err := domain.NewRoutingTable(domain.RoutingFile{
		Version: 1,
		Services: map[string]domain.ServiceRoutes{
			"workflow": {Source: workflowSource, TenantAdmins: &yes, Events: []string{"Shared"}},
			"llm":      {Source: llmSource, TenantAdmins: &no, Events: []string{"Shared"}},
		},
	})
	require.NoError(t, err)
	return table
}

func TestFanoutService_Ingest_TheSameTypeUnderTwoServicesRoutesBySource(t *testing.T) {
	payload := recipients(users("u1"), dept("dept-a"))

	store := &fakeEventStore{}
	name, err := NewFanoutService(store, twoServiceTable(t)).Ingest(context.Background(),
		IngestInput{Source: llmSource, EventType: "Shared", TenantID: "t", Payload: payload})

	require.NoError(t, err)
	assert.Equal(t, "llm.Shared", name)
	require.Len(t, store.appended, 1)
	assert.Equal(t, "llm.Shared", store.appended[0].eventType)
	assert.Empty(t, store.broadcast, "the llm section does not reach tenant administrators")
	require.Len(t, store.department, 1)

	store = &fakeEventStore{}
	name, err = NewFanoutService(store, twoServiceTable(t)).Ingest(context.Background(),
		IngestInput{Source: workflowSource, EventType: "Shared", TenantID: "t", Payload: payload})

	require.NoError(t, err)
	assert.Equal(t, "workflow.Shared", name)
	require.Len(t, store.broadcast, 1)
	assert.Equal(t, "workflow.Shared", store.broadcast[0].eventType)
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

func TestFanoutService_Ingest_ReadsTheRecipientList(t *testing.T) {
	for name, tc := range map[string]struct {
		payload   string
		wantUsers []string
		wantDepts []string
	}{
		"users and departments": {
			`{"recipients":[{"kind":"user","id":"u1"},{"kind":"department","id":"d1"},{"kind":"user","id":"u2"},{"kind":"department","id":"d2"}]}`,
			[]string{"u1", "u2"}, []string{"d1", "d2"},
		},
		"repeats are written once": {
			`{"recipients":[{"kind":"user","id":"u1"},{"kind":"user","id":"u1"},{"kind":"department","id":"d1"},{"kind":"department","id":"d1"}]}`,
			[]string{"u1"}, []string{"d1"},
		},
		"a user and a department with the same id are both written": {
			`{"recipients":[{"kind":"user","id":"x"},{"kind":"department","id":"x"}]}`,
			[]string{"x"}, []string{"x"},
		},
		"extra entry fields are ignored": {
			`{"recipients":[{"kind":"user","id":"u1","label":"Ann"}]}`,
			[]string{"u1"}, nil,
		},
		"empty list": {`{"recipients":[]}`, nil, nil},
		"null":       {`{"recipients":null}`, nil, nil},
		"missing":    {`{"other":1}`, nil, nil},
	} {
		t.Run(name, func(t *testing.T) {
			store := &fakeEventStore{}

			_, err := NewFanoutService(store, workflowTable(t)).Ingest(context.Background(),
				IngestInput{Source: workflowSource, EventType: eventTaskCreated, TenantID: "t", Payload: []byte(tc.payload)})

			require.NoError(t, err)
			var gotUsers, gotDepts []string
			for _, e := range store.appended {
				gotUsers = append(gotUsers, e.userID)
			}
			for _, e := range store.department {
				gotDepts = append(gotDepts, e.departmentID)
			}
			assert.Equal(t, tc.wantUsers, gotUsers)
			assert.Equal(t, tc.wantDepts, gotDepts)
			assert.Len(t, store.broadcast, 1)
		})
	}
}

func TestFanoutService_Ingest_RefusesAPayloadOfTheWrongShape(t *testing.T) {
	for name, payload := range map[string]string{
		"not an object":          `["u1"]`,
		"null":                   `null`,
		"not JSON":               `{`,
		"recipients is a number": `{"recipients":7}`,
		"recipients is a map":    `{"recipients":{"kind":"user","id":"u1"}}`,
		"entry is a string":      `{"recipients":["u1"]}`,
		"entry is null":          `{"recipients":[null]}`,
		"kind missing":           `{"recipients":[{"id":"u1"}]}`,
		"kind unknown":           `{"recipients":[{"kind":"group","id":"u1"}]}`,
		"kind is a number":       `{"recipients":[{"kind":1,"id":"u1"}]}`,
		"id missing":             `{"recipients":[{"kind":"user"}]}`,
		"id empty":               `{"recipients":[{"kind":"user","id":""}]}`,
		"id is a number":         `{"recipients":[{"kind":"user","id":7}]}`,
		"id over 128":            `{"recipients":[{"kind":"user","id":"` + strings.Repeat("a", 129) + `"}]}`,
		"one bad among good":     `{"recipients":[{"kind":"user","id":"u1"},{"kind":"team","id":"t1"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			store := &fakeEventStore{}

			_, err := NewFanoutService(store, workflowTable(t)).Ingest(context.Background(),
				IngestInput{Source: workflowSource, EventType: eventTaskCreated, TenantID: "t", Payload: []byte(payload)})

			assert.ErrorIs(t, err, ErrInvalidPayload)
			assert.Empty(t, store.appended, "nothing is written for a payload that does not decode")
			assert.Empty(t, store.broadcast)
			assert.Empty(t, store.department)
		})
	}
}
