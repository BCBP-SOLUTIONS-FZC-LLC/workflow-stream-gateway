package domain

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func boolPtr(b bool) *bool { return &b }

func validRoutingFile() RoutingFile {
	return RoutingFile{
		Version: 1,
		Services: map[string]ServiceRoutes{
			"workflow": {
				Source:       "workflow-execution-svc",
				TenantAdmins: boolPtr(true),
				Events:       []string{"WorkflowTaskCreated", "WorkflowInstancePaused"},
			},
			"llm": {
				Source:       "llm-retrieval-generation",
				TenantAdmins: boolPtr(false),
				Events:       []string{"WorkflowTaskCreated", "GenerationProgressed"},
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
	assert.True(t, route.TenantAdmins)
	assert.Equal(t, "workflow.WorkflowTaskCreated", route.StreamEventType())

	other, ok := table.Lookup("llm-retrieval-generation", "WorkflowTaskCreated")
	require.True(t, ok, "the same type name under another service is its own route")
	assert.Equal(t, "llm", other.Service)
	assert.False(t, other.TenantAdmins)
	assert.Equal(t, "llm.WorkflowTaskCreated", other.StreamEventType())
}

func TestNewRoutingTable_AnUnlistedPairIsUnknown(t *testing.T) {
	table, err := NewRoutingTable(validRoutingFile())
	require.NoError(t, err)

	for _, pair := range [][2]string{
		{"workflow-execution-svc", "WorkflowInstanceStarted"},
		{"llm-retrieval-generation", "WorkflowInstancePaused"},
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
	assert.Empty(t, table.EventTypes("tender"))
}

func TestNewRoutingTable_RefusesABrokenFile(t *testing.T) {
	cases := map[string]struct {
		mutate func(*RoutingFile)
		want   string
	}{
		"version 0":       {func(f *RoutingFile) { f.Version = 0 }, "version must be 1"},
		"version 2":       {func(f *RoutingFile) { f.Version = 2 }, "version must be 1"},
		"no services":     {func(f *RoutingFile) { f.Services = nil }, "at least one service"},
		"uppercase name":  {func(f *RoutingFile) { f.Services["Llm"] = f.Services["llm"]; delete(f.Services, "llm") }, `service "Llm"`},
		"name with a dot": {func(f *RoutingFile) { f.Services["l.lm"] = f.Services["llm"]; delete(f.Services, "llm") }, `service "l.lm"`},
		"empty source": {func(f *RoutingFile) {
			s := f.Services["llm"]
			s.Source = ""
			f.Services["llm"] = s
		}, `service "llm": source is required`},
		"shared source": {func(f *RoutingFile) {
			s := f.Services["llm"]
			s.Source = "workflow-execution-svc"
			f.Services["llm"] = s
		}, `source "workflow-execution-svc" is used by two services`},
		"no tenant_admins": {func(f *RoutingFile) {
			s := f.Services["llm"]
			s.TenantAdmins = nil
			f.Services["llm"] = s
		}, `service "llm": tenant_admins is required`},
		"no events": {func(f *RoutingFile) {
			s := f.Services["llm"]
			s.Events = nil
			f.Services["llm"] = s
		}, `service "llm": at least one event`},
		"event type with a space": {func(f *RoutingFile) {
			s := f.Services["llm"]
			s.Events = append(s.Events, "Generation Done")
			f.Services["llm"] = s
		}, `service "llm": event type "Generation Done"`},
		"event type with a newline": {func(f *RoutingFile) {
			s := f.Services["llm"]
			s.Events = append(s.Events, "GenerationDone\ndata: x")
			f.Services["llm"] = s
		}, `service "llm": event type`},
		"empty event type": {func(f *RoutingFile) {
			s := f.Services["llm"]
			s.Events = append(s.Events, "")
			f.Services["llm"] = s
		}, `service "llm": an event type is empty`},
		"repeated event type": {func(f *RoutingFile) {
			s := f.Services["llm"]
			s.Events = append(s.Events, "GenerationProgressed")
			f.Services["llm"] = s
		}, `service "llm": event type "GenerationProgressed" is listed twice`},
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

func TestRecipient_Validate(t *testing.T) {
	cases := map[string]struct {
		r    Recipient
		want string
	}{
		"user":                {Recipient{Kind: RecipientUser, ID: "u1"}, ""},
		"department":          {Recipient{Kind: RecipientDepartment, ID: "d1"}, ""},
		"id of 128":           {Recipient{Kind: RecipientUser, ID: strings.Repeat("a", 128)}, ""},
		"id of 129":           {Recipient{Kind: RecipientUser, ID: strings.Repeat("a", 129)}, "id must be 1 to 128 characters"},
		"empty id":            {Recipient{Kind: RecipientUser, ID: ""}, "id must be 1 to 128 characters"},
		"missing kind":        {Recipient{ID: "u1"}, `kind "" must be user or department`},
		"other kind":          {Recipient{Kind: "group", ID: "u1"}, `kind "group" must be user or department`},
		"kind in other cases": {Recipient{Kind: "User", ID: "u1"}, `kind "User" must be user or department`},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := tc.r.Validate()

			if tc.want == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}
