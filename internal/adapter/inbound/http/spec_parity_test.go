package http

import (
	"os"
	"regexp"
	"sort"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

const (
	openAPISpec  = "../../../../docs/swagger/openapi.yaml"
	asyncAPISpec = "../../../../api/asyncapi.yaml"
)

var operationalRoutes = map[string]bool{"/healthz": true, "/readyz": true}

func specOperations(t *testing.T) []string {
	t.Helper()
	doc, err := openapi3.NewLoader().LoadFromFile(openAPISpec)
	require.NoError(t, err)
	param := regexp.MustCompile(`\{([^}]+)\}`)
	var ops []string
	for p, item := range doc.Paths.Map() {
		if operationalRoutes[p] {
			continue
		}
		for method := range item.Operations() {
			ops = append(ops, method+" "+param.ReplaceAllString(p, ":$1"))
		}
	}
	require.NotEmpty(t, ops)
	sort.Strings(ops)
	return ops
}

func routerOperations() []string {
	gin.SetMode(gin.TestMode)
	var ops []string
	for _, r := range NewRouter(testRouterConfig()).Routes() {
		if operationalRoutes[r.Path] {
			continue
		}
		ops = append(ops, r.Method+" "+r.Path)
	}
	sort.Strings(ops)
	return ops
}

func TestOpenAPIMatchesRoutes(t *testing.T) {
	assert.Equal(t, specOperations(t), routerOperations(),
		"docs/swagger/openapi.yaml and the router must list the same operations")
}

func TestAsyncAPIMessagesMatchKnownEventTypes(t *testing.T) {
	raw, err := os.ReadFile(asyncAPISpec)
	require.NoError(t, err)
	var doc struct {
		Components struct {
			Messages map[string]struct {
				Name string `yaml:"name"`
			} `yaml:"messages"`
		} `yaml:"components"`
	}
	require.NoError(t, yaml.Unmarshal(raw, &doc))

	var documented []string
	for _, m := range doc.Components.Messages {
		documented = append(documented, m.Name)
	}
	sort.Strings(documented)
	known := append([]string(nil), usecase.KnownEventTypes()...)
	sort.Strings(known)
	assert.Equal(t, known, documented, "api/asyncapi.yaml must document exactly the event types the service routes")
}
