package chart_test

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const (
	chartDir                    = "../../deploy/helm"
	routerFile                  = "../../internal/adapter/inbound/http/router.go"
	authorizationPolicyTemplate = "templates/authorizationpolicy.yaml"
	internalPrefix              = "/api/v1/internal"
	releaseNamespace            = "workflow-app"
)

// prodSettings are what a production render needs: the chart refuses to render without them.
var prodSettings = []string{
	"--set", "env.VALKEY_ADDR=valkey.platform.svc:6379",
	"--set", "existingSecret=workflow-stream-gateway-secrets",
}

type chartObject struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		Ports []struct {
			Name        string `yaml:"name"`
			Port        int    `yaml:"port"`
			AppProtocol string `yaml:"appProtocol"`
		} `yaml:"ports"`
		Template struct {
			Spec struct {
				ServiceAccountName string `yaml:"serviceAccountName"`
			} `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
}

type policyRule struct {
	From []struct {
		Source struct {
			Principals []string `yaml:"principals"`
		} `yaml:"source"`
	} `yaml:"from"`
	To []struct {
		Operation struct {
			Paths    []string `yaml:"paths"`
			NotPaths []string `yaml:"notPaths"`
		} `yaml:"operation"`
	} `yaml:"to"`
}

type authorizationPolicy struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		Selector struct {
			MatchLabels map[string]string `yaml:"matchLabels"`
		} `yaml:"selector"`
		Action string       `yaml:"action"`
		Rules  []policyRule `yaml:"rules"`
	} `yaml:"spec"`
}

// helmRender runs `helm template` for a release with the production settings and returns the output.
func helmRender(t *testing.T, release string, extra ...string) (string, error) {
	t.Helper()
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm is not installed")
	}
	args := append([]string{"template", release, chartDir, "--namespace", releaseNamespace}, prodSettings...)
	out, err := exec.Command(helm, append(args, extra...)...).CombinedOutput()
	return string(out), err
}

// decodeAll decodes every document of a render.
func decodeAll[T any](t *testing.T, out string) []T {
	t.Helper()
	dec := yaml.NewDecoder(strings.NewReader(out))
	var docs []T
	for {
		var doc T
		err := dec.Decode(&doc)
		if errors.Is(err, io.EOF) {
			return docs
		}
		require.NoError(t, err, out)
		docs = append(docs, doc)
	}
}

func renderPolicy(t *testing.T, release string, extra ...string) authorizationPolicy {
	t.Helper()
	out, err := helmRender(t, release, append([]string{"-s", authorizationPolicyTemplate}, extra...)...)
	require.NoError(t, err, out)
	docs := decodeAll[authorizationPolicy](t, out)
	require.Len(t, docs, 1)
	return docs[0]
}

func writeValues(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "values.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

func principal(namespace, serviceAccount string) string {
	return "cluster.local/ns/" + namespace + "/sa/" + serviceAccount
}

// wantRule is one ALLOW rule on an internal route group: its callers and its paths together.
type wantRule struct {
	name       string
	principals []string
	paths      []string
}

func defaultInternalRules() []wantRule {
	return []wantRule{
		{"events", []string{principal("workflow-app", "workflow-events-consumer")}, []string{"/api/v1/internal/events"}},
	}
}

// policyProblems compares a decoded policy with the rules it must have: rule 1 admits what is
// outside the internal routes with no source, and every other rule carries one group's principals
// and paths together.
func policyProblems(rules []policyRule, want []wantRule) []string {
	if len(rules) != len(want)+1 {
		return []string{"rule count differs from the expected groups plus the admit-everything-else rule"}
	}
	var problems []string
	first := rules[0]
	if len(first.From) != 0 || len(first.To) != 1 ||
		!equalStrings(first.To[0].Operation.NotPaths, []string{"/api/v1/internal/*"}) || len(first.To[0].Operation.Paths) != 0 {
		problems = append(problems, "rule 1 must be only to.operation.notPaths [/api/v1/internal/*]")
	}
	for i, w := range want {
		r := rules[i+1]
		if len(r.From) != 1 || len(r.To) != 1 {
			problems = append(problems, w.name+": an internal rule must hold one from and one to")
			continue
		}
		if !equalStrings(r.From[0].Source.Principals, w.principals) {
			problems = append(problems, w.name+": principals differ")
		}
		if !equalStrings(r.To[0].Operation.Paths, w.paths) || len(r.To[0].Operation.NotPaths) != 0 {
			problems = append(problems, w.name+": paths differ")
		}
	}
	return problems
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestHelmAuthorizationPolicy_RendersExactlyTheExpectedRules(t *testing.T) {
	p := renderPolicy(t, "wf")

	assert.Equal(t, "security.istio.io/v1", p.APIVersion)
	assert.Equal(t, "AuthorizationPolicy", p.Kind)
	assert.Equal(t, "ALLOW", p.Spec.Action)
	assert.Equal(t, "workflow-stream-gateway", p.Metadata.Name)
	assert.Empty(t, policyProblems(p.Spec.Rules, defaultInternalRules()))
}

func TestHelmAuthorizationPolicy_RuleOneAdmitsOnlyTheNonInternalPaths(t *testing.T) {
	p := renderPolicy(t, "wf")
	require.NotEmpty(t, p.Spec.Rules)

	assert.Empty(t, p.Spec.Rules[0].From, "rule 1 names no source: the gateway, probes and the scrape must stay admitted")
	require.Len(t, p.Spec.Rules[0].To, 1)
	assert.Equal(t, []string{"/api/v1/internal/*"}, p.Spec.Rules[0].To[0].Operation.NotPaths)
	assert.Empty(t, p.Spec.Rules[0].To[0].Operation.Paths)
}

func TestHelmAuthorizationPolicy_SelectsThePodsOfTheRelease(t *testing.T) {
	p := renderPolicy(t, "wf")

	assert.Equal(t, map[string]string{
		"app.kubernetes.io/name":     "workflow-stream-gateway",
		"app.kubernetes.io/instance": "wf",
	}, p.Spec.Selector.MatchLabels)
}

func TestHelmAuthorizationPolicy_AGroupWithNoCallersRendersNoRule(t *testing.T) {
	values := writeValues(t, `authorizationPolicy:
  internalRoutes:
    - name: events
      paths: ["/api/v1/internal/events"]
      callers: []
`)
	p := renderPolicy(t, "wf", "-f", values)

	assert.Empty(t, policyProblems(p.Spec.Rules, nil), "the events group has no caller, so no rule admits it and the path stays closed")
}

// The rule check must fail on the unsafe shapes it exists to catch.
func TestHelmAuthorizationPolicy_TheRuleCheckCatchesPrincipalsAndPathsSplitAcrossRules(t *testing.T) {
	p := renderPolicy(t, "wf")
	want := defaultInternalRules()
	require.Empty(t, policyProblems(p.Spec.Rules, want))

	t.Run("principals in one rule, paths in another", func(t *testing.T) {
		original := p.Spec.Rules[1]
		principalsOnly := policyRule{From: original.From}
		pathsOnly := policyRule{To: original.To}
		mutated := []policyRule{p.Spec.Rules[0], principalsOnly, pathsOnly}

		assert.NotEmpty(t, policyProblems(mutated, want))
	})
	t.Run("the internal path opened to every caller", func(t *testing.T) {
		mutated := append([]policyRule(nil), p.Spec.Rules...)
		mutated[1] = policyRule{To: mutated[1].To}

		assert.NotEmpty(t, policyProblems(mutated, want))
	})
	t.Run("a probe path added to the notPaths list", func(t *testing.T) {
		mutated := append([]policyRule(nil), p.Spec.Rules...)
		mutated[0].To = append(mutated[0].To[:0:0], mutated[0].To...)
		mutated[0].To[0].Operation.NotPaths = []string{"/api/v1/internal/*", "/healthz"}

		assert.NotEmpty(t, policyProblems(mutated, want))
	})
	t.Run("the caller in the wrong namespace", func(t *testing.T) {
		wrong := []wantRule{{"events", []string{principal("elsewhere", "workflow-events-consumer")}, want[0].paths}}

		assert.NotEmpty(t, policyProblems(p.Spec.Rules, wrong))
	})
}

func TestHelmAuthorizationPolicy_IsNotRenderedWhenDisabled(t *testing.T) {
	out, err := helmRender(t, "wf", "-s", authorizationPolicyTemplate, "--set", "authorizationPolicy.enabled=false", "--set-string", "env.APP_ENV=dev")
	require.Error(t, err)
	assert.Contains(t, out, "could not find template "+authorizationPolicyTemplate, "a disabled policy renders nothing, so helm finds no template to show")

	out, err = helmRender(t, "wf", "--set", "authorizationPolicy.enabled=false", "--set-string", "env.APP_ENV=dev")
	require.NoError(t, err, out)
	assert.NotContains(t, out, "kind: AuthorizationPolicy")
}

func TestHelmAuthorizationPolicy_TheRenderRefusesAPolicyThatIsOffInProdAndStaging(t *testing.T) {
	for _, env := range []string{"prod", "staging"} {
		out, err := helmRender(t, "wf", "--set", "authorizationPolicy.enabled=false", "--set-string", "env.APP_ENV="+env)
		require.Error(t, err, "env.APP_ENV=%s with the policy off leaves the internal route to the NetworkPolicy alone", env)
		assert.Contains(t, out, "authorizationPolicy.allowDisabled", env)

		out, err = helmRender(t, "wf", "--set", "authorizationPolicy.enabled=false", "--set", "authorizationPolicy.allowDisabled=true", "--set-string", "env.APP_ENV="+env)
		require.NoError(t, err, out)
		assert.NotContains(t, out, "kind: AuthorizationPolicy")
	}
	for _, env := range []string{"local", "dev", "test"} {
		out, err := helmRender(t, "wf", "--set", "authorizationPolicy.enabled=false", "--set-string", "env.APP_ENV="+env)
		require.NoError(t, err, out)
	}
	out, err := helmRender(t, "wf", "--set-string", "env.APP_ENV=prod")
	require.NoError(t, err, out, "the policy on is the default and renders in prod")
}

func TestHelmNotes_StateTheMeshRequirement(t *testing.T) {
	body, err := os.ReadFile(filepath.Join(chartDir, "templates", "NOTES.txt"))
	require.NoError(t, err)
	notes := string(body)

	assert.Contains(t, notes, "sidecar")
	assert.Contains(t, notes, "mutual TLS")
	assert.Contains(t, notes, "refused on the internal paths")
	assert.Contains(t, notes, "PeerAuthentication")
	assert.Contains(t, notes, "Platform")
}

func TestHelmNames_TheServiceAndServiceAccountDoNotDependOnTheReleaseName(t *testing.T) {
	for _, release := range []string{"wf", "stream", "a-much-longer-release-name"} {
		out, err := helmRender(t, release)
		require.NoError(t, err, out)

		var services, accounts, workloads []string
		for _, o := range decodeAll[chartObject](t, out) {
			switch o.Kind {
			case "Service":
				services = append(services, o.Metadata.Name)
			case "ServiceAccount":
				accounts = append(accounts, o.Metadata.Name)
			case "Deployment":
				workloads = append(workloads, o.Spec.Template.Spec.ServiceAccountName)
			}
		}

		assert.Equal(t, []string{"workflow-stream-gateway"}, services, release)
		assert.Equal(t, []string{"workflow-stream-gateway"}, accounts, release)
		assert.Equal(t, []string{"workflow-stream-gateway"}, workloads, "%s: the pods run as the ServiceAccount the policies name", release)
	}
}

func TestHelmServices_TheHTTPAndMetricsPortsAreNamedAsHTTP(t *testing.T) {
	out, err := helmRender(t, "wf")
	require.NoError(t, err, out)

	seen := 0
	for _, o := range decodeAll[chartObject](t, out) {
		if o.Kind != "Service" {
			continue
		}
		for _, port := range o.Spec.Ports {
			assert.Equal(t, "http", port.AppProtocol, "port %s: without it a mesh may treat the port as TCP and path rules match nothing", port.Name)
			seen++
		}
	}
	assert.Equal(t, 2, seen, "the http and metrics ports")
}

// route is one internal route read from the router source.
type route struct {
	method string
	path   string
}

var internalRouteLine = regexp.MustCompile(`internalGroup\.(GET|POST|PUT|PATCH|DELETE)\("([^"]+)"`)

func routerInternalRoutes(t *testing.T) []route {
	t.Helper()
	body, err := os.ReadFile(routerFile)
	require.NoError(t, err)
	require.Contains(t, string(body), `r.Group("`+internalPrefix+`")`, "the internal group moved: update internalPrefix")

	var routes []route
	for _, m := range internalRouteLine.FindAllStringSubmatch(string(body), -1) {
		routes = append(routes, route{method: m[1], path: internalPrefix + m[2]})
	}
	return routes
}

// istioPathMatches follows Istio's path matching: exact, or one "*" at the start or the end. A
// pattern "/x/*" matches paths below "/x/" and never "/x" itself.
func istioPathMatches(pattern, path string) bool {
	switch {
	case strings.HasSuffix(pattern, "*"):
		return strings.HasPrefix(path, strings.TrimSuffix(pattern, "*"))
	case strings.HasPrefix(pattern, "*"):
		return strings.HasSuffix(path, strings.TrimPrefix(pattern, "*"))
	default:
		return path == pattern
	}
}

// A route added to router.go with no caller list must fail here, not be silently closed.
func TestHelmAuthorizationPolicy_EveryInternalRouteBelongsToAGroupAndEveryGroupPathIsARoute(t *testing.T) {
	routes := routerInternalRoutes(t)
	require.NotEmpty(t, routes, "no internal route was read from the router, so the check proved nothing")
	rules := renderPolicy(t, "wf").Spec.Rules[1:]

	for _, r := range routes {
		admitted := 0
		for _, rule := range rules {
			for _, to := range rule.To {
				for _, p := range to.Operation.Paths {
					if istioPathMatches(p, r.path) {
						admitted++
					}
				}
			}
		}
		assert.Equal(t, 1, admitted, "%s %s must be named by exactly one caller group", r.method, r.path)
	}
	for _, rule := range rules {
		for _, to := range rule.To {
			for _, p := range to.Operation.Paths {
				matched := false
				for _, r := range routes {
					matched = matched || istioPathMatches(p, r.path)
				}
				assert.True(t, matched, "policy path %s matches no internal route", p)
			}
		}
	}
}

func TestIstioPathMatches_ASlashStarPatternNeverMatchesItsOwnPrefix(t *testing.T) {
	assert.False(t, istioPathMatches("/api/v1/internal/*", "/api/v1/internal"))
	assert.True(t, istioPathMatches("/api/v1/internal/*", "/api/v1/internal/events"))
	assert.True(t, istioPathMatches("/api/v1/internal/events", "/api/v1/internal/events"))
	assert.False(t, istioPathMatches("/api/v1/internal/events", "/api/v1/internal/events/x"))
}

// The shared token is gone: no template reads it and the values do not carry it.
func TestHelmChart_CarriesNoSharedToken(t *testing.T) {
	var files []string
	var matches []string
	removed := []string{"WORKFLOW_STREAM_GATEWAY_TOKEN", "INTERNAL_API_TOKEN", "x-internal-token"}

	err := filepath.WalkDir(chartDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		body, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		files = append(files, path)
		for _, name := range removed {
			if bytes.Contains(body, []byte(name)) {
				matches = append(matches, path+" names "+name)
			}
		}
		return nil
	})

	require.NoError(t, err)
	require.NotEmpty(t, files, "the chart directory was not found, so the check proved nothing")
	assert.Contains(t, files, filepath.Join(chartDir, "values.yaml"))
	assert.Contains(t, files, filepath.Join(chartDir, "templates", "authorizationpolicy.yaml"))
	assert.Empty(t, matches)
}
