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

const minimal = "version: 1\nservices:\n  workflow:\n    source: s\n    events:\n      T: {recipients: [u]}\n"

func TestLoad_ReadsAValidFile(t *testing.T) {
	table, err := Load(writeFile(t, minimal))
	require.NoError(t, err)

	route, ok := table.Lookup("s", "T")
	require.True(t, ok)
	assert.Equal(t, "workflow.T", route.StreamEventType())
}

func TestLoad_TheShippedFileRoutesTheSixteenWorkflowEvents(t *testing.T) {
	table, err := Load(defaultFile)
	require.NoError(t, err)

	assert.Len(t, table.EventTypes("workflow"), 16)
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
		"misspelt key":  "version: 1\nservice:\n  workflow:\n    source: s\n    events:\n      T: {recipients: [u]}\n",
		"service field": "version: 1\nservices:\n  workflow:\n    source: s\n    queue: q\n    events:\n      T: {recipients: [u]}\n",
		"event field":   "version: 1\nservices:\n  workflow:\n    source: s\n    events:\n      T: {recipients: [u], tier: all}\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeFile(t, content))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not found")
		})
	}
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
	_, err := Load(writeFile(t, "version: 2\nservices:\n  workflow:\n    source: s\n    events:\n      T: {recipients: [u]}\n"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "version must be 1")
}
