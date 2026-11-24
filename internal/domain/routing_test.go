package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validRoutingFile() RoutingFile {
	return RoutingFile{
		Version: 1,
		Services: map[string]ServiceRoutes{
			"workflow": {
				Source: "workflow-execution-svc",
				Events: map[string]EventRoute{
					"WorkflowTaskCreated":    {Recipients: []string{"assignee_user_ids"}, Department: "department_id"},
					"WorkflowInstancePaused": {Recipients: []string{"started_by_user_id"}},
				},
			},
			"tender": {
				Source: "tender-svc",
				Events: map[string]EventRoute{
					"WorkflowTaskCreated": {Recipients: []string{"owner_user_id"}},
				},
			},
		},
	}
}

func TestNewRoutingTable_LooksUpByServiceSourceAndEventType(t *testing.T) {
	table, err := NewRoutingTable(validRoutingFile())
	require.NoError(t, err)

	route, ok := table.Lookup("workflow-execution-svc", "WorkflowTaskCreated")
	require.True(t, ok)
	assert.Equal(t, "workflow", route.Service)
	assert.Equal(t, []string{"assignee_user_ids"}, route.Recipients)
	assert.Equal(t, "department_id", route.Department)
	assert.Equal(t, "workflow.WorkflowTaskCreated", route.StreamEventType())

	other, ok := table.Lookup("tender-svc", "WorkflowTaskCreated")
	require.True(t, ok, "the same type name under another service is its own route")
	assert.Equal(t, "tender", other.Service)
	assert.Equal(t, []string{"owner_user_id"}, other.Recipients)
	assert.Equal(t, "tender.WorkflowTaskCreated", other.StreamEventType())
}

func TestNewRoutingTable_AnUnlistedPairIsUnknown(t *testing.T) {
	table, err := NewRoutingTable(validRoutingFile())
	require.NoError(t, err)

	for _, pair := range [][2]string{
		{"workflow-execution-svc", "WorkflowInstanceStarted"},
		{"tender-svc", "WorkflowInstancePaused"},
		{"unknown-svc", "WorkflowTaskCreated"},
		{"", ""},
	} {
		_, ok := table.Lookup(pair[0], pair[1])
		assert.False(t, ok, "%v", pair)
	}
}

func TestNewRoutingTable_ListsTheEventTypesOfAService(t *testing.T) {
	table, err := NewRoutingTable(validRoutingFile())
	require.NoError(t, err)

	assert.Equal(t, []string{"WorkflowInstancePaused", "WorkflowTaskCreated"}, table.EventTypes("workflow"))
	assert.Empty(t, table.EventTypes("llm"))
}

func TestNewRoutingTable_RefusesABrokenFile(t *testing.T) {
	cases := map[string]struct {
		mutate func(*RoutingFile)
		want   string
	}{
		"version 0":       {func(f *RoutingFile) { f.Version = 0 }, "version must be 1"},
		"version 2":       {func(f *RoutingFile) { f.Version = 2 }, "version must be 1"},
		"no services":     {func(f *RoutingFile) { f.Services = nil }, "at least one service"},
		"uppercase name":  {func(f *RoutingFile) { f.Services["Tender"] = f.Services["tender"]; delete(f.Services, "tender") }, `service "Tender"`},
		"name with a dot": {func(f *RoutingFile) { f.Services["ten.der"] = f.Services["tender"]; delete(f.Services, "tender") }, `service "ten.der"`},
		"empty source": {func(f *RoutingFile) {
			s := f.Services["tender"]
			s.Source = ""
			f.Services["tender"] = s
		}, `service "tender": source is required`},
		"shared source": {func(f *RoutingFile) {
			s := f.Services["tender"]
			s.Source = "workflow-execution-svc"
			f.Services["tender"] = s
		}, `source "workflow-execution-svc" is used by two services`},
		"no events": {func(f *RoutingFile) {
			s := f.Services["tender"]
			s.Events = nil
			f.Services["tender"] = s
		}, `service "tender": at least one event`},
		"event type with a space": {func(f *RoutingFile) {
			f.Services["tender"].Events["Tender Awarded"] = EventRoute{Recipients: []string{"x"}}
		}, `service "tender": event type "Tender Awarded"`},
		"event type with a newline": {func(f *RoutingFile) {
			f.Services["tender"].Events["TenderAwarded\ndata: x"] = EventRoute{Recipients: []string{"x"}}
		}, `service "tender": event type`},
		"empty event type": {func(f *RoutingFile) {
			f.Services["tender"].Events[""] = EventRoute{Recipients: []string{"x"}}
		}, `service "tender": an event type is empty`},
		"no recipients": {func(f *RoutingFile) {
			f.Services["tender"].Events["TenderAwarded"] = EventRoute{}
		}, `"tender" event "TenderAwarded": at least one recipient field`},
		"empty recipient field": {func(f *RoutingFile) {
			f.Services["tender"].Events["TenderAwarded"] = EventRoute{Recipients: []string{"a", ""}}
		}, `"tender" event "TenderAwarded": a recipient field is empty`},
		"repeated recipient field": {func(f *RoutingFile) {
			f.Services["tender"].Events["TenderAwarded"] = EventRoute{Recipients: []string{"a", "a"}}
		}, `"tender" event "TenderAwarded": recipient field "a" is listed twice`},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := validRoutingFile()
			tc.mutate(&f)

			_, err := NewRoutingTable(f)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}
