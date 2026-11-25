package routingfile

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const defaultFile = "../../../../config/routing.yaml"

func writeFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "routing.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

const minimal = "version: 1\nservices:\n  workflow:\n    source: s\n    callers: [ns/sa]\n    tenant_admins: true\n    events: [T]\n"

func TestLoad_ReadsAValidFile(t *testing.T) {
	table, err := Load(writeFile(t, minimal))
	require.NoError(t, err)

	route, ok := table.Lookup("s", "T")
	require.True(t, ok)
	assert.Equal(t, "workflow.T", route.StreamEventType())
}

func TestLoad_TheShippedFileRoutesTheWorkflowAndLLMEvents(t *testing.T) {
	table, err := Load(defaultFile)
	require.NoError(t, err)

	assert.Len(t, table.EventTypes("workflow"), 16)
	assert.Len(t, table.EventTypes("llm"), 10)
	workflow, ok := table.Lookup("workflow-execution-svc", "WorkflowTaskCreated")
	require.True(t, ok)
	assert.True(t, workflow.TenantAdmins)
	llm, ok := table.Lookup("llm-retrieval-generation", "GenerationProgressed")
	require.True(t, ok)
	assert.False(t, llm.TenantAdmins)
}

func TestLoad_RefusesAMissingOrUnreadableFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "absent.yaml"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "absent.yaml")
}

func TestLoad_AnEmptyFileSaysItIsEmpty(t *testing.T) {
	for name, content := range map[string]string{"empty": "", "comment only": "# nothing\n"} {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeFile(t, content))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "is empty")
		})
	}
}

func TestLoad_RefusesAnUnknownOrMisspeltKey(t *testing.T) {
	for name, content := range map[string]string{
		"top level":     minimal + "extra: true\n",
		"misspelt key":  "version: 1\nservice:\n  workflow:\n    source: s\n    callers: [ns/sa]\n    tenant_admins: true\n    events: [T]\n",
		"service field": "version: 1\nservices:\n  workflow:\n    source: s\n    callers: [ns/sa]\n    queue: q\n    tenant_admins: true\n    events: [T]\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeFile(t, content))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not found")
		})
	}
}

func TestLoad_RefusesTheRetiredPerEventFieldForm(t *testing.T) {
	_, err := Load(writeFile(t, "version: 1\nservices:\n  workflow:\n    source: s\n    callers: [ns/sa]\n    tenant_admins: true\n"+
		"    events:\n      T: {recipients: [u], department: d}\n"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "parsing")
}

func TestLoad_RefusesAServiceWithoutTenantAdmins(t *testing.T) {
	_, err := Load(writeFile(t, "version: 1\nservices:\n  workflow:\n    source: s\n    callers: [ns/sa]\n    events: [T]\n"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "tenant_admins is required")
}

func TestLoad_RefusesATenantAdminsThatIsNotTrueOrFalse(t *testing.T) {
	_, err := Load(writeFile(t, "version: 1\nservices:\n  workflow:\n    source: s\n    callers: [ns/sa]\n    tenant_admins: maybe\n    events: [T]\n"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "parsing")
}

func TestLoad_RefusesASecondDocument(t *testing.T) {
	_, err := Load(writeFile(t, minimal+"---\nversion: 1\n"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "one YAML document")
}

func TestLoad_RefusesAFileThatIsNotYAML(t *testing.T) {
	_, err := Load(writeFile(t, "services: [unclosed\n"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "parsing")
}

func TestLoad_RefusesAFileTheRoutingRulesReject(t *testing.T) {
	_, err := Load(writeFile(t, "version: 2\nservices:\n  workflow:\n    source: s\n    callers: [ns/sa]\n    tenant_admins: true\n    events: [T]\n"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "version must be 1")
}
