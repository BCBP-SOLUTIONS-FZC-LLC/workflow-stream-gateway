package routingfile

import (
	"os"
	"path/filepath"
	"testing"
)

func FuzzLoad_ReturnsATableOrAnError(f *testing.F) {
	shipped, err := os.ReadFile("../../../../config/routing.yaml")
	if err != nil {
		f.Fatal(err)
	}
	f.Add(shipped)
	for _, seed := range []string{
		"", "version: 1", "version: 1\nservices: {}", "---\n---", "version: 2\nservices:\n  a:\n    source: s",
		"version: 1\nservices:\n  w:\n    source: s\n    callers: [ns/sa]\n    tenant_admins: true\n    events: [E]\nextra: 1",
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		path := filepath.Join(t.TempDir(), "routing.yaml")
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		table, err := Load(path)
		if (table == nil) == (err == nil) {
			t.Fatalf("Load gave table %v and error %v", table, err)
		}
	})
}
