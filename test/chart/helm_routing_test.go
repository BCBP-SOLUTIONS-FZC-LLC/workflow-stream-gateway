package chart_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/outbound/routingfile"
)

type renderedDoc struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Data map[string]string `yaml:"data"`
	Spec struct {
		Template struct {
			Metadata struct {
				Annotations map[string]string `yaml:"annotations"`
			} `yaml:"metadata"`
			Spec struct {
				Containers []struct {
					Env []struct {
						Name  string `yaml:"name"`
						Value string `yaml:"value"`
					} `yaml:"env"`
					VolumeMounts []struct {
						Name      string `yaml:"name"`
						MountPath string `yaml:"mountPath"`
						ReadOnly  bool   `yaml:"readOnly"`
					} `yaml:"volumeMounts"`
				} `yaml:"containers"`
				Volumes []struct {
					Name      string `yaml:"name"`
					ConfigMap struct {
						Name string `yaml:"name"`
					} `yaml:"configMap"`
				} `yaml:"volumes"`
			} `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
}

func TestHelmRouting_TheRenderedFileMatchesTheShippedOneUnderTheSameRules(t *testing.T) {
	out, err := helmRender(t, "gw")
	require.NoError(t, err, out)

	var routing string
	for _, doc := range decodeAll[renderedDoc](t, out) {
		if doc.Kind == "ConfigMap" && doc.Metadata.Name == "workflow-stream-gateway-routing" {
			routing = doc.Data["routing.yaml"]
		}
	}
	require.NotEmpty(t, routing, "the chart renders the routing ConfigMap")
	path := filepath.Join(t.TempDir(), "routing.yaml")
	require.NoError(t, os.WriteFile(path, []byte(routing), 0o600))

	chart, err := routingfile.Load(path)
	require.NoError(t, err, "the chart's routing values load under the service's own rules")
	shipped, err := routingfile.Load("../../config/routing.yaml")
	require.NoError(t, err)

	for service, source := range map[string]string{
		"workflow": "workflow-execution-svc",
		"llm":      "llm-retrieval-generation",
	} {
		require.NotEmpty(t, shipped.EventTypes(service), service)
		require.Equal(t, shipped.EventTypes(service), chart.EventTypes(service), service)
		for _, eventType := range shipped.EventTypes(service) {
			want, _ := shipped.Lookup(source, eventType)
			got, ok := chart.Lookup(source, eventType)
			require.True(t, ok, eventType)
			assert.Equal(t, want, got, eventType)
		}
	}
}

func TestHelmRouting_TheFileIsMountedReadOnlyAndAChangeRollsThePods(t *testing.T) {
	out, err := helmRender(t, "gw")
	require.NoError(t, err, out)

	var found bool
	for _, doc := range decodeAll[renderedDoc](t, out) {
		if doc.Kind != "Deployment" {
			continue
		}
		found = true
		assert.NotEmpty(t, doc.Spec.Template.Metadata.Annotations["checksum/routing"])
		require.Len(t, doc.Spec.Template.Spec.Containers, 1)
		c := doc.Spec.Template.Spec.Containers[0]
		env := map[string]string{}
		for _, e := range c.Env {
			env[e.Name] = e.Value
		}
		assert.Equal(t, "/etc/workflow-stream-gateway/routing.yaml", env["ROUTING_CONFIG_PATH"])
		require.Len(t, c.VolumeMounts, 1)
		assert.Equal(t, "routing", c.VolumeMounts[0].Name)
		assert.Equal(t, "/etc/workflow-stream-gateway", c.VolumeMounts[0].MountPath)
		assert.True(t, c.VolumeMounts[0].ReadOnly)
		require.Len(t, doc.Spec.Template.Spec.Volumes, 1)
		assert.Equal(t, "workflow-stream-gateway-routing", doc.Spec.Template.Spec.Volumes[0].ConfigMap.Name)
	}
	require.True(t, found, "the chart renders a Deployment")

	changed, err := helmRender(t, "gw", "--set", "routing.services.workflow.source=other-svc")
	require.NoError(t, err, changed)
	assert.NotEqual(t, checksum(t, out), checksum(t, changed), "a routing change changes the annotation, so the pods roll")
}

func checksum(t *testing.T, out string) string {
	t.Helper()
	for _, doc := range decodeAll[renderedDoc](t, out) {
		if doc.Kind == "Deployment" {
			return doc.Spec.Template.Metadata.Annotations["checksum/routing"]
		}
	}
	t.Fatal("no Deployment in the render")
	return ""
}

func TestHelmRouting_ThePodsSidecarSetsTheCallerCertificateHeaderItself(t *testing.T) {
	out, err := helmRender(t, "gw")
	require.NoError(t, err, out)

	for _, doc := range decodeAll[renderedDoc](t, out) {
		if doc.Kind != "Deployment" {
			continue
		}
		config := doc.Spec.Template.Metadata.Annotations["proxy.istio.io/config"]
		assert.Contains(t, config, "forwardedClientCert: SANITIZE_SET",
			"the sidecar replaces any client-sent X-Forwarded-Client-Cert, so the gateway reads only the mesh's caller identity")
		return
	}
	t.Fatal("no Deployment in the render")
}

func TestHelmRouting_EveryRoutingCallerIsAdmittedByTheMeshPolicy(t *testing.T) {
	out, err := helmRender(t, "gw")
	require.NoError(t, err, out)
	var routing string
	for _, doc := range decodeAll[renderedDoc](t, out) {
		if doc.Kind == "ConfigMap" && doc.Metadata.Name == "workflow-stream-gateway-routing" {
			routing = doc.Data["routing.yaml"]
		}
	}
	var file struct {
		Services map[string]struct {
			Callers []string `yaml:"callers"`
		} `yaml:"services"`
	}
	require.NoError(t, yaml.Unmarshal([]byte(routing), &file))
	policy := renderPolicy(t, "gw")

	admitted := map[string]bool{}
	for _, rule := range policy.Spec.Rules {
		for _, from := range rule.From {
			for _, p := range from.Source.Principals {
				admitted[p] = true
			}
		}
	}
	for service, s := range file.Services {
		require.NotEmpty(t, s.Callers, service)
		for _, caller := range s.Callers {
			ns, sa, _ := strings.Cut(caller, "/")
			assert.True(t, admitted[principal(ns, sa)], "%s caller %s is not admitted by the AuthorizationPolicy", service, caller)
		}
	}
}
