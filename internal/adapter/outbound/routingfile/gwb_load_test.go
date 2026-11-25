package routingfile

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const gwBMinimal = "version: 1\nservices:\n  workflow:\n    source: s\n    callers: [ns/sa]\n    tenant_admins: true\n    events: [T]\n"

func gwBWrite(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "routing.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

// A file that cannot be read fails with "routing: reading <path>: " around the OS error, and the
// OS error stays reachable.
func TestGwB_Load_ReadFailuresNameThePathAndKeepTheCause(t *testing.T) {
	dir := t.TempDir()
	unreadable := filepath.Join(dir, "locked.yaml")
	require.NoError(t, os.WriteFile(unreadable, []byte(gwBMinimal), 0o600))
	require.NoError(t, os.Chmod(unreadable, 0o000))
	t.Cleanup(func() { _ = os.Chmod(unreadable, 0o600) })
	canRead := func() bool { _, err := os.ReadFile(unreadable); return err == nil }
	missing := filepath.Join(dir, "absent.yaml")

	cases := []struct {
		name    string
		path    string
		wantIs  error
		skipped bool
	}{
		{"a file that does not exist", missing, fs.ErrNotExist, false},
		{"a path whose directory does not exist", filepath.Join(dir, "no-such-dir", "routing.yaml"), fs.ErrNotExist, false},
		{"a directory", dir, nil, false},
		{"a file without read permission", unreadable, fs.ErrPermission, canRead()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skipped {
				t.Skip("this user can read a mode 000 file (running as root)")
			}

			table, err := Load(tc.path)

			require.Error(t, err)
			assert.Nil(t, table)
			assert.True(t, strings.HasPrefix(err.Error(), "routing: reading "+tc.path+": "), err.Error())
			if tc.wantIs != nil {
				assert.ErrorIs(t, err, tc.wantIs)
			} else {
				assert.NotErrorIs(t, err, fs.ErrNotExist)
				assert.ErrorContains(t, err, "is a directory")
			}
		})
	}
}

// A file that is not valid for the decoder fails with "routing: parsing <path>: " and the
// decoder's own error stays reachable.
func TestGwB_Load_SyntaxAndTypeErrorsAreParsingErrors(t *testing.T) {
	cases := []struct {
		name    string
		content string
		wantTyp bool
	}{
		{"a tab used as indentation", "version: 1\nservices:\n\tworkflow:\n\t\tsource: s\n", false},
		{"version that is a word", "version: x\nservices:\n  workflow:\n    source: s\n    callers: [ns/sa]\n    tenant_admins: true\n    events: [T]\n", true},
		{"services as a list", "version: 1\nservices:\n  - workflow\n", true},
		{"events as a number", "version: 1\nservices:\n  workflow:\n    source: s\n    callers: [ns/sa]\n    tenant_admins: true\n    events: 3\n", true},
		{"an unclosed flow list", "services: [unclosed\n", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := gwBWrite(t, tc.content)

			table, err := Load(path)

			require.Error(t, err)
			assert.Nil(t, table)
			assert.True(t, strings.HasPrefix(err.Error(), "routing: parsing "+path+": "), err.Error())
			var typeErr *yaml.TypeError
			assert.Equal(t, tc.wantTyp, errors.As(err, &typeErr), "a wrong-typed value is a yaml type error; broken syntax is not")
		})
	}
}

// A file the routing rules reject fails with "routing: <path>: <reason>"; the reason is the
// domain error, reachable by unwrapping.
func TestGwB_Load_DomainRejectionsAreReportedAfterThePath(t *testing.T) {
	cases := []struct {
		name       string
		content    string
		wantReason string
	}{
		{"wrong version", "version: 2\nservices:\n  workflow:\n    source: s\n    callers: [ns/sa]\n    tenant_admins: true\n    events: [T]\n",
			"version must be 1, got 2"},
		{"no services", "version: 1\nservices: {}\n",
			"at least one service is required"},
		{"no tenant_admins", "version: 1\nservices:\n  workflow:\n    source: s\n    callers: [ns/sa]\n    events: [T]\n",
			`service "workflow": tenant_admins is required`},
		{"a caller that is not namespace/account", "version: 1\nservices:\n  workflow:\n    source: s\n    callers: [bad]\n    tenant_admins: true\n    events: [T]\n",
			`service "workflow": caller "bad" must be <namespace>/<service account>`},
		{"an event listed twice", "version: 1\nservices:\n  workflow:\n    source: s\n    callers: [ns/sa]\n    tenant_admins: true\n    events: [T, T]\n",
			`service "workflow": event type "T" is listed twice`},
		{"one source used by two services", "version: 1\nservices:\n  a:\n    source: s\n    callers: [ns/sa]\n    tenant_admins: true\n    events: [T]\n  b:\n    source: s\n    callers: [ns/sa]\n    tenant_admins: false\n    events: [U]\n",
			`source "s" is used by two services (a, b)`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := gwBWrite(t, tc.content)

			table, err := Load(path)

			require.Error(t, err)
			assert.Nil(t, table)
			assert.Equal(t, "routing: "+path+": "+tc.wantReason, err.Error())
			inner := errors.Unwrap(err)
			require.Error(t, inner, "the domain error is wrapped, not flattened")
			assert.Equal(t, tc.wantReason, inner.Error())
		})
	}
}

// A valid file gives a table whose lookups return the route's service, tenant-admin flag and
// callers, per source; an unlisted pair is not found. The path is cleaned before it is read.
func TestGwB_Load_AValidFileGivesRoutesWithTheirServiceAdminFlagAndCallers(t *testing.T) {
	content := `version: 1
services:
  workflow:
    source: wf-src
    callers: [workflow-app/consumer, workflow-app/replayer]
    tenant_admins: true
    events: [Created, Done]
  llm:
    source: llm-src
    callers: [llm/retrieval]
    tenant_admins: false
    events: [Progressed]
`
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "routing.yaml"), []byte(content), 0o600))
	paths := map[string]string{
		"a clean path":                              filepath.Join(dir, "routing.yaml"),
		"a path with a dot segment":                 dir + "/./routing.yaml",
		"a path through a directory that is absent": dir + "/no-such-dir/../routing.yaml",
	}
	for name, path := range paths {
		t.Run(name, func(t *testing.T) {
			table, err := Load(path)
			require.NoError(t, err)

			created, ok := table.Lookup("wf-src", "Created")
			require.True(t, ok)
			assert.Equal(t, "workflow", created.Service)
			assert.Equal(t, "Created", created.EventType)
			assert.True(t, created.TenantAdmins)
			assert.Equal(t, []string{"workflow-app/consumer", "workflow-app/replayer"}, created.Callers)
			assert.True(t, created.Admits("workflow-app/replayer"))
			assert.False(t, created.Admits("llm/retrieval"))
			assert.False(t, created.Admits(""))

			progressed, ok := table.Lookup("llm-src", "Progressed")
			require.True(t, ok)
			assert.Equal(t, "llm", progressed.Service)
			assert.False(t, progressed.TenantAdmins)
			assert.Equal(t, []string{"llm/retrieval"}, progressed.Callers)

			_, ok = table.Lookup("llm-src", "Created")
			assert.False(t, ok, "an event type is routed only under its own source")
			_, ok = table.Lookup("wf-src", "Unlisted")
			assert.False(t, ok)
			assert.Equal(t, []string{"Created", "Done"}, table.EventTypes("workflow"))
		})
	}
}
