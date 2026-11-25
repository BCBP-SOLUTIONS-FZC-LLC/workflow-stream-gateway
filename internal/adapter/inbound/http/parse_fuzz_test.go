package http

import (
	"regexp"
	"strings"
	"testing"
)

var callerShape = regexp.MustCompile(`^[^/]+/[^/]+$`)

func FuzzCallerFromClientCert_NamesOneCallerOrNone(f *testing.F) {
	for _, seed := range []string{
		"", workflowConsumerCert,
		`By=spiffe://td/ns/gw/sa/gw;URI=spiffe://td/ns/workflow-app/sa/workflow-events-consumer`,
		`Subject="a,b";URI=spiffe://td/ns/x/sa/y`, `Subject="unterminated;URI=spiffe://td/ns/x/sa/y`,
		`URI=spiffe://td/ns/x/sa/y,URI=spiffe://td/ns/llm/sa/llm-retrieval-generation`,
		`URI=spiffe://td/ns//sa/y`, `URI=spiffe://td/ns/x/sa/y/extra`,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, header string) {
		caller := callerFromClientCert(header)
		if caller != "" && !callerShape.MatchString(caller) {
			t.Fatalf("caller %q from %q is not <namespace>/<service account>", caller, header)
		}
	})
}

func FuzzDataFields_NeverEndsTheEventOrAddsAField(f *testing.F) {
	for _, seed := range []string{"", "{}", "a\nb", "a\r\nb", "a\rb", "\n\n", "id: forged\nevent: x", "data: y\r\n\r\n"} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		out := dataFields(data)
		if !strings.HasSuffix(out, "\n") || strings.Contains(out, "\r") {
			t.Fatalf("data fields %q from %q", out, data)
		}
		for _, line := range strings.Split(strings.TrimSuffix(out, "\n"), "\n") {
			if !strings.HasPrefix(line, "data: ") {
				t.Fatalf("line %q from %q is not a data field", line, data)
			}
		}
	})
}

func FuzzSplitDepartments_KeepsNonEmptyIDs(f *testing.F) {
	for _, seed := range []string{"", "d1:admin", "d1:admin, ,bad,:r,d2:", ",,,", "a:b:c"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		for _, d := range splitDepartments(raw) {
			if d.DepartmentID == "" || strings.ContainsAny(d.DepartmentID, ",:") {
				t.Fatalf("department %q kept from %q", d.DepartmentID, raw)
			}
		}
	})
}
