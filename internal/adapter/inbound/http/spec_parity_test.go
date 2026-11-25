package http

import (
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"sort"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const (
	openAPISpec  = "../../../../docs/swagger/openapi.yaml"
	asyncAPISpec = "../../../../api/asyncapi.yaml"
)

func specOperations(t *testing.T) []string {
	t.Helper()
	doc, err := openapi3.NewLoader().LoadFromFile(openAPISpec)
	require.NoError(t, err)
	param := regexp.MustCompile(`\{([^}]+)\}`)
	var ops []string
	for p, item := range doc.Paths.Map() {
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
		ops = append(ops, r.Method+" "+r.Path)
	}
	sort.Strings(ops)
	return ops
}

func TestOpenAPIMatchesRoutes(t *testing.T) {
	assert.Equal(t, specOperations(t), routerOperations(),
		"docs/swagger/openapi.yaml and the router must list the same operations")
}

type asyncAPIDoc struct {
	Channels map[string]struct {
		Messages map[string]struct {
			Ref string `yaml:"$ref"`
		} `yaml:"messages"`
	} `yaml:"channels"`
	Components struct {
		Messages map[string]struct {
			Name    string `yaml:"name"`
			Payload struct {
				Ref string `yaml:"$ref"`
			} `yaml:"payload"`
		} `yaml:"messages"`
		Schemas map[string]struct {
			Required []string `yaml:"required"`
		} `yaml:"schemas"`
	} `yaml:"components"`
}

func loadAsyncAPI(t *testing.T) asyncAPIDoc {
	t.Helper()
	raw, err := os.ReadFile(asyncAPISpec)
	require.NoError(t, err)
	var doc asyncAPIDoc
	require.NoError(t, yaml.Unmarshal(raw, &doc))
	return doc
}

func TestAsyncAPIChannelsMatchTheShippedRoutes(t *testing.T) {
	doc := loadAsyncAPI(t)
	routes := shippedRoutes(t)

	for channel, service := range map[string]string{"wf/workflow/dash": "workflow", "llm/generation": "llm"} {
		var documented []string
		for name := range doc.Channels[channel].Messages {
			documented = append(documented, name)
		}
		sort.Strings(documented)
		known := routes.EventTypes(service)
		require.NotEmpty(t, known, service)
		assert.Equal(t, known, documented, "channel %s of api/asyncapi.yaml must list exactly the %s types config/routing.yaml routes", channel, service)
	}

	var all []string
	for _, service := range []string{"workflow", "llm"} {
		all = append(all, routes.EventTypes(service)...)
	}
	sort.Strings(all)
	var messages []string
	for _, m := range doc.Components.Messages {
		messages = append(messages, m.Name)
	}
	sort.Strings(messages)
	assert.Equal(t, all, messages, "api/asyncapi.yaml must document a message for exactly the routed types")
}

func TestAsyncAPIEveryMessageCarriesTheRecipientList(t *testing.T) {
	doc := loadAsyncAPI(t)

	for key, m := range doc.Components.Messages {
		assert.Equal(t, "#/components/schemas/RoutedPayload", m.Payload.Ref, key)
	}
	assert.Contains(t, doc.Components.Schemas["RoutedPayload"].Required, "recipients")
	assert.ElementsMatch(t, []string{"kind", "id"}, doc.Components.Schemas["Recipient"].Required)
}

func TestTicketMintDocumentsTheStatusItAnswersWithoutIdentityHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	NewRouter(testRouterConfig()).ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/v1/stream/tickets", nil))
	require.Equal(t, http.StatusUnauthorized, w.Code)

	doc, err := openapi3.NewLoader().LoadFromFile(openAPISpec)
	require.NoError(t, err)
	op := doc.Paths.Find("/api/v1/stream/tickets").Post
	assert.NotNil(t, op.Responses.Status(w.Code), "the ticket mint answers %d without the gateway identity headers; docs/swagger/openapi.yaml must document it", w.Code)
}
