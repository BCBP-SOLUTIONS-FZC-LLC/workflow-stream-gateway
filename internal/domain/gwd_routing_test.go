package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func gwDFile() RoutingFile {
	tenantAdmins := true
	return RoutingFile{
		Version: RoutingFormatVersion,
		Services: map[string]ServiceRoutes{
			"workflow": {
				Source:       "workflow-execution-svc",
				Callers:      []string{"workflow-app/workflow-events-consumer"},
				TenantAdmins: &tenantAdmins,
				Events:       []string{"WorkflowTaskCreated", "WorkflowInstancePaused"},
			},
		},
	}
}

// gwDWithService returns a valid file with the "workflow" service changed by mutate.
func gwDWithService(mutate func(*ServiceRoutes)) RoutingFile {
	f := gwDFile()
	svc := f.Services["workflow"]
	mutate(&svc)
	f.Services["workflow"] = svc
	return f
}

func TestGwD_NewRoutingTable_AnEmptyServicesMapIsRefused(t *testing.T) {
	cases := []struct {
		name     string
		services map[string]ServiceRoutes
	}{
		{"nil map", nil},
		{"empty non-nil map", map[string]ServiceRoutes{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := gwDFile()
			f.Services = tc.services

			table, err := NewRoutingTable(f)

			assert.EqualError(t, err, "at least one service is required")
			assert.Nil(t, table)
		})
	}
}

func TestGwD_NewRoutingTable_ServiceNamePattern(t *testing.T) {
	cases := []struct {
		name    string
		service string
		valid   bool
	}{
		{"leading digit", "1workflow", false},
		{"underscore", "work_flow", false},
		{"empty name", "", false},
		{"leading dash", "-workflow", false},
		{"space", "work flow", false},
		{"uppercase", "Workflow", false},
		{"single letter", "a", true},
		{"letter then digit", "a9", true},
		{"dashes and digits", "a-1-b-", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := gwDFile()
			f.Services = map[string]ServiceRoutes{tc.service: f.Services["workflow"]}

			table, err := NewRoutingTable(f)

			if tc.valid {
				require.NoError(t, err)
				assert.Equal(t, []string{"WorkflowInstancePaused", "WorkflowTaskCreated"}, table.EventTypes(tc.service))
				return
			}
			require.Error(t, err)
			assert.Nil(t, table)
			assert.Contains(t, err.Error(), `service "`+tc.service+`": name must be lowercase letters, digits and dashes`)
		})
	}
}

func TestGwD_NewRoutingTable_OneInvalidServiceFailsTheWholeTable(t *testing.T) {
	broken := map[string]func(*ServiceRoutes){
		"no callers":        func(s *ServiceRoutes) { s.Callers = nil },
		"no events":         func(s *ServiceRoutes) { s.Events = nil },
		"bad event type":    func(s *ServiceRoutes) { s.Events = []string{"Fine", "not fine"} },
		"repeated event":    func(s *ServiceRoutes) { s.Events = []string{"Fine", "Fine"} },
		"no tenant_admins":  func(s *ServiceRoutes) { s.TenantAdmins = nil },
		"empty source":      func(s *ServiceRoutes) { s.Source = "" },
		"source already in": func(s *ServiceRoutes) { s.Source = "alpha-svc" },
	}
	// "alpha" sorts before and "zeta" after the broken "middle", so valid services are built both
	// before and after the failure.
	for name, mutate := range broken {
		t.Run(name, func(t *testing.T) {
			valid := gwDFile().Services["workflow"]
			middle := valid
			middle.Source = "middle-svc"
			mutate(&middle)
			zeta := valid
			zeta.Source = "zeta-svc"
			alpha := valid
			alpha.Source = "alpha-svc"
			f := gwDFile()
			f.Services = map[string]ServiceRoutes{"alpha": alpha, "middle": middle, "zeta": zeta}

			table, err := NewRoutingTable(f)

			require.Error(t, err)
			assert.Nil(t, table, "no partial table is returned")
			assert.Contains(t, err.Error(), "middle")
		})
	}
}

func TestGwD_NewRoutingTable_AnEmptyCallerListIsRefused(t *testing.T) {
	f := gwDWithService(func(s *ServiceRoutes) { s.Callers = []string{} })

	table, err := NewRoutingTable(f)

	assert.EqualError(t, err, `service "workflow": at least one caller is required`)
	assert.Nil(t, table)
}

func TestGwD_NewRoutingTable_CallerPattern(t *testing.T) {
	cases := []struct {
		name   string
		caller string
		valid  bool
	}{
		{"no slash", "workflow-events-consumer", false},
		{"uppercase namespace", "Workflow-App/consumer", false},
		{"uppercase account", "workflow-app/Consumer", false},
		{"leading dash in namespace", "-app/consumer", false},
		{"trailing dash in namespace", "app-/consumer", false},
		{"leading dash in account", "app/-consumer", false},
		{"trailing dash in account", "app/consumer-", false},
		{"trailing dot in account", "app/consumer.", false},
		{"leading dot in account", "app/.consumer", false},
		{"two slashes", "app/consumer/extra", false},
		{"dot in namespace", "my.app/consumer", false},
		{"empty namespace", "/consumer", false},
		{"empty account", "app/", false},
		{"underscore in account", "app/my_consumer", false},
		{"dot in account", "app/consumer.v2", true},
		{"dots and dashes in account", "app/events-consumer.v2-b", true},
		{"dash inside namespace", "my-app/consumer", true},
		{"single characters", "a/b", true},
		{"digits", "0/9", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := gwDWithService(func(s *ServiceRoutes) { s.Callers = []string{tc.caller} })

			table, err := NewRoutingTable(f)

			if tc.valid {
				require.NoError(t, err)
				route, ok := table.Lookup("workflow-execution-svc", "WorkflowTaskCreated")
				require.True(t, ok)
				assert.Equal(t, []string{tc.caller}, route.Callers)
				return
			}
			require.Error(t, err)
			assert.Nil(t, table)
			assert.Contains(t, err.Error(), `service "workflow": caller "`+tc.caller+`" must be <namespace>/<service account>`)
		})
	}
}

func TestGwD_NewRoutingTable_EventTypePattern(t *testing.T) {
	cases := []struct {
		name      string
		eventType string
		valid     bool
	}{
		{"leading digit", "1Created", false},
		{"slash", "Task/Created", false},
		{"colon", "Task:Created", false},
		{"leading dash", "-Created", false},
		{"leading underscore", "_Created", false},
		{"leading dot", ".Created", false},
		{"trailing colon", "Created:", false},
		{"single letter", "A", true},
		{"mixed case with dot dash underscore and digit", "Task.Created-v2_b", true},
		{"lower case", "created", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := gwDWithService(func(s *ServiceRoutes) { s.Events = []string{tc.eventType} })

			table, err := NewRoutingTable(f)

			if tc.valid {
				require.NoError(t, err)
				route, ok := table.Lookup("workflow-execution-svc", tc.eventType)
				require.True(t, ok)
				assert.Equal(t, tc.eventType, route.EventType)
				return
			}
			require.Error(t, err)
			assert.Nil(t, table)
			assert.Contains(t, err.Error(), `service "workflow": event type "`+tc.eventType+`" must be letters, digits, dots, dashes and underscores`)
		})
	}
}

func TestGwD_RoutingTable_LookupCarriesTheFileValuesAndOwnsItsCallers(t *testing.T) {
	f := gwDFile()
	callers := []string{"workflow-app/workflow-events-consumer", "workflow-app/second"}
	events := []string{"WorkflowTaskCreated", "WorkflowInstancePaused"}
	svc := f.Services["workflow"]
	svc.Callers = callers
	svc.Events = events
	f.Services["workflow"] = svc

	table, err := NewRoutingTable(f)
	require.NoError(t, err)
	// Changing the slices of the file after the table is built must not reach the table.
	callers[0] = "mutated/after-build"
	callers[1] = "mutated/after-build-2"
	events[0] = "Mutated"

	for _, eventType := range []string{"WorkflowTaskCreated", "WorkflowInstancePaused"} {
		route, ok := table.Lookup("workflow-execution-svc", eventType)

		require.True(t, ok, eventType)
		assert.Equal(t, "workflow", route.Service)
		assert.Equal(t, eventType, route.EventType)
		assert.True(t, route.TenantAdmins)
		assert.Equal(t, []string{"workflow-app/workflow-events-consumer", "workflow-app/second"}, route.Callers)
	}
	_, ok := table.Lookup("workflow-execution-svc", "Mutated")
	assert.False(t, ok, "a later change of the input events adds no route")
	assert.Equal(t, []string{"WorkflowInstancePaused", "WorkflowTaskCreated"}, table.EventTypes("workflow"))
	assert.Equal(t, []string{"Mutated", "WorkflowInstancePaused"}, events, "the table sorts a copy, not the caller's slice")
}

func TestGwD_RoutingTable_TwoEventsOfAServiceDoNotShareACallerSlice(t *testing.T) {
	table, err := NewRoutingTable(gwDFile())
	require.NoError(t, err)
	first, ok := table.Lookup("workflow-execution-svc", "WorkflowTaskCreated")
	require.True(t, ok)
	second, ok := table.Lookup("workflow-execution-svc", "WorkflowInstancePaused")
	require.True(t, ok)

	first.Callers[0] = "changed/by-a-reader"

	assert.Equal(t, []string{"workflow-app/workflow-events-consumer"}, second.Callers)
}

func TestGwD_IsCanonicalID(t *testing.T) {
	const id = "0190c6a2-7b3e-7f00-9a1b-2c3d4e5f6a7b"
	cases := []struct {
		name string
		id   string
		want bool
	}{
		{"lower-case uuid", id, true},
		{"all zeros", "00000000-0000-0000-0000-000000000000", true},
		{"all f", "ffffffff-ffff-ffff-ffff-ffffffffffff", true},
		{"upper-case", "0190C6A2-7B3E-7F00-9A1B-2C3D4E5F6A7B", false},
		{"one upper-case digit", "0190c6a2-7b3e-7f00-9a1b-2c3d4e5f6A7b", false},
		{"no dashes", "0190c6a27b3e7f009a1b2c3d4e5f6a7b", false},
		{"braces", "{" + id + "}", false},
		{"urn", "urn:uuid:" + id, false},
		{"trailing newline", id + "\n", false},
		{"leading newline", "\n" + id, false},
		{"leading space", " " + id, false},
		{"trailing space", id + " ", false},
		{"empty", "", false},
		{"non-hex digit", "0190c6a2-7b3e-7f00-9a1b-2c3d4e5f6a7g", false},
		{"short last group", "0190c6a2-7b3e-7f00-9a1b-2c3d4e5f6a7", false},
		{"long last group", "0190c6a2-7b3e-7f00-9a1b-2c3d4e5f6a7b0", false},
		{"suffix after a key separator", id + ":x", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsCanonicalID(tc.id))
		})
	}
}
