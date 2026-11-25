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
			"workflow": {Source: workflowSource, Callers: []string{workflowCaller}, TenantAdmins: &yes, Events: []string{"Shared"}},
			"llm":      {Source: llmSource, Callers: []string{llmCaller}, TenantAdmins: &no, Events: []string{"Shared"}},
		},
	})
	require.NoError(t, err)
	return table
}

func TestFanoutService_Ingest_TheSameTypeUnderTwoServicesRoutesBySource(t *testing.T) {
	payload := recipients(users("00000000-0000-4000-8000-0000000000a1"), dept("00000000-0000-4000-8000-0000000000d0"))

	store := &fakeEventStore{}
	name, err := NewFanoutService(store, twoServiceTable(t), CallerCheckOff).Ingest(context.Background(),
		IngestInput{Source: llmSource, EventType: "Shared", TenantID: "00000000-0000-4000-8000-0000000000a1", Payload: payload})

	require.NoError(t, err)
	assert.Equal(t, "llm.Shared", name)
	require.Len(t, store.appended, 1)
	assert.Equal(t, "llm.Shared", store.appended[0].eventType)
	assert.Empty(t, store.broadcast, "the llm section does not reach tenant administrators")
	require.Len(t, store.department, 1)

	store = &fakeEventStore{}
	name, err = NewFanoutService(store, twoServiceTable(t), CallerCheckOff).Ingest(context.Background(),
		IngestInput{Source: workflowSource, EventType: "Shared", TenantID: "00000000-0000-4000-8000-0000000000a1", Payload: payload})

	require.NoError(t, err)
	assert.Equal(t, "workflow.Shared", name)
	require.Len(t, store.broadcast, 1)
	assert.Equal(t, "workflow.Shared", store.broadcast[0].eventType)
}

func TestFanoutService_Ingest_ATypeUnderAnotherSourceIsUnknown(t *testing.T) {
	store := &fakeEventStore{}

	name, err := NewFanoutService(store, workflowTable(t), CallerCheckOff).Ingest(context.Background(),
		IngestInput{Source: "tender-svc", EventType: eventTaskCreated, TenantID: "00000000-0000-4000-8000-0000000000a1", Payload: []byte(`{}`)})

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
			`{"recipients":[{"kind":"user","id":"00000000-0000-4000-8000-0000000000a1"},{"kind":"department","id":"00000000-0000-4000-8000-0000000000d1"},{"kind":"user","id":"00000000-0000-4000-8000-0000000000a2"},{"kind":"department","id":"00000000-0000-4000-8000-0000000000d2"}]}`,
			[]string{"00000000-0000-4000-8000-0000000000a1", "00000000-0000-4000-8000-0000000000a2"}, []string{"00000000-0000-4000-8000-0000000000d1", "00000000-0000-4000-8000-0000000000d2"},
		},
		"repeats are written once": {
			`{"recipients":[{"kind":"user","id":"00000000-0000-4000-8000-0000000000a1"},{"kind":"user","id":"00000000-0000-4000-8000-0000000000a1"},{"kind":"department","id":"00000000-0000-4000-8000-0000000000d1"},{"kind":"department","id":"00000000-0000-4000-8000-0000000000d1"}]}`,
			[]string{"00000000-0000-4000-8000-0000000000a1"}, []string{"00000000-0000-4000-8000-0000000000d1"},
		},
		"a user and a department with the same id are both written": {
			`{"recipients":[{"kind":"user","id":"00000000-0000-4000-8000-0000000000e1"},{"kind":"department","id":"00000000-0000-4000-8000-0000000000e1"}]}`,
			[]string{"00000000-0000-4000-8000-0000000000e1"}, []string{"00000000-0000-4000-8000-0000000000e1"},
		},
		"extra entry fields are ignored": {
			`{"recipients":[{"kind":"user","id":"00000000-0000-4000-8000-0000000000a1","label":"Ann"}]}`,
			[]string{"00000000-0000-4000-8000-0000000000a1"}, nil,
		},
		"empty list": {`{"recipients":[]}`, nil, nil},
	} {
		t.Run(name, func(t *testing.T) {
			store := &fakeEventStore{}

			_, err := NewFanoutService(store, workflowTable(t), CallerCheckOff).Ingest(context.Background(),
				IngestInput{Source: workflowSource, EventType: eventTaskCreated, TenantID: "00000000-0000-4000-8000-0000000000a1", Payload: []byte(tc.payload)})

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
		"not an object":           `["00000000-0000-4000-8000-0000000000a1"]`,
		"null":                    `null`,
		"not JSON":                `{`,
		"recipients missing":      `{"other":1}`,
		"recipients null":         `{"recipients":null}`,
		"recipients is a number":  `{"recipients":7}`,
		"recipients is a map":     `{"recipients":{"kind":"user","id":"00000000-0000-4000-8000-0000000000a1"}}`,
		"entry is a string":       `{"recipients":["00000000-0000-4000-8000-0000000000a1"]}`,
		"entry is null":           `{"recipients":[null]}`,
		"kind missing":            `{"recipients":[{"id":"00000000-0000-4000-8000-0000000000a1"}]}`,
		"kind unknown":            `{"recipients":[{"kind":"group","id":"00000000-0000-4000-8000-0000000000a1"}]}`,
		"kind is a number":        `{"recipients":[{"kind":1,"id":"00000000-0000-4000-8000-0000000000a1"}]}`,
		"id missing":              `{"recipients":[{"kind":"user"}]}`,
		"id empty":                `{"recipients":[{"kind":"user","id":""}]}`,
		"id is a number":          `{"recipients":[{"kind":"user","id":7}]}`,
		"id not a uuid":           `{"recipients":[{"kind":"user","id":"u1"}]}`,
		"id upper-case":           `{"recipients":[{"kind":"user","id":"` + strings.ToUpper(uuidA) + `"}]}`,
		"id with a key separator": `{"recipients":[{"kind":"user","id":"` + uuidA + `:x"}]}`,
		"one bad among good":      `{"recipients":[{"kind":"user","id":"00000000-0000-4000-8000-0000000000a1"},{"kind":"team","id":"t1"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			store := &fakeEventStore{}

			_, err := NewFanoutService(store, workflowTable(t), CallerCheckOff).Ingest(context.Background(),
				IngestInput{Source: workflowSource, EventType: eventTaskCreated, TenantID: "00000000-0000-4000-8000-0000000000a1", Payload: []byte(payload)})

			assert.ErrorIs(t, err, ErrInvalidPayload)
			assert.Empty(t, store.appended, "nothing is written for a payload that does not decode")
			assert.Empty(t, store.broadcast)
			assert.Empty(t, store.department)
		})
	}
}

const (
	workflowCaller = "workflow-app/workflow-events-consumer"
	llmCaller      = "llm/llm-retrieval-generation"
	uuidA          = "00000000-0000-4000-8000-0000000000a1"
)

func TestFanoutService_Ingest_ACallerPublishesOnlyForItsOwnSection(t *testing.T) {
	for name, tc := range map[string]struct {
		source, caller string
		allowed        bool
	}{
		"workflow consumer for workflow":  {workflowSource, workflowCaller, true},
		"llm consumer for llm":            {llmSource, llmCaller, true},
		"llm consumer posing as workflow": {workflowSource, llmCaller, false},
		"workflow consumer posing as llm": {llmSource, workflowCaller, false},
		"no caller identity":              {workflowSource, "", false},
		"unknown caller":                  {workflowSource, "other/sa", false},
	} {
		t.Run(name, func(t *testing.T) {
			store := &fakeEventStore{}

			_, err := NewFanoutService(store, twoServiceTable(t), CallerCheckOn).Ingest(context.Background(), IngestInput{
				Source: tc.source, EventType: "Shared", TenantID: "00000000-0000-4000-8000-0000000000a1", Caller: tc.caller,
				Payload: recipients(users(uuidA)),
			})

			if tc.allowed {
				require.NoError(t, err)
				assert.Len(t, store.appended, 1)
				return
			}
			assert.ErrorIs(t, err, ErrCallerNotAllowed)
			assert.Empty(t, store.appended, "nothing is written for a caller outside the section")
			assert.Empty(t, store.broadcast)
			assert.Empty(t, store.department)
		})
	}
}

func TestFanoutService_Ingest_AnUnlistedPairIsUnknownWhoeverTheCaller(t *testing.T) {
	_, err := NewFanoutService(&fakeEventStore{}, twoServiceTable(t), CallerCheckOn).Ingest(context.Background(),
		IngestInput{Source: "tender-svc", EventType: "Shared", TenantID: "00000000-0000-4000-8000-0000000000a1", Caller: "other/sa", Payload: []byte(`{}`)})

	assert.ErrorIs(t, err, ErrUnknownEventType)
}

func TestFanoutService_Ingest_WithTheCallerCheckOffAnyCallerIsAdmitted(t *testing.T) {
	store := &fakeEventStore{}

	_, err := NewFanoutService(store, twoServiceTable(t), CallerCheckOff).Ingest(context.Background(),
		IngestInput{Source: workflowSource, EventType: "Shared", TenantID: "00000000-0000-4000-8000-0000000000a1", Payload: recipients(users(uuidA))})

	require.NoError(t, err)
	assert.Len(t, store.appended, 1)
}
