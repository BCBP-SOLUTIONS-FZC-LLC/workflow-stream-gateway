package http

import (
	"context"
	"crypto/rand"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/outbound/routingfile"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability/obstest"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

type fakeEventStore struct {
	appendCalls     int
	broadcastCalls  int
	departmentCalls int
	forceErr        error
}

func (f *fakeEventStore) AppendEvent(_ context.Context, _, _, _ string, _ []byte) error {
	f.appendCalls++
	if f.forceErr != nil {
		return f.forceErr
	}
	return nil
}

func (f *fakeEventStore) AppendBroadcastEvent(_ context.Context, _, _ string, _ []byte) error {
	f.broadcastCalls++
	if f.forceErr != nil {
		return f.forceErr
	}
	return nil
}

func (f *fakeEventStore) AppendDepartmentEvent(_ context.Context, _, _, _ string, _ []byte) error {
	f.departmentCalls++
	if f.forceErr != nil {
		return f.forceErr
	}
	return nil
}

const shippedRoutingFile = "../../../../config/routing.yaml"

var loadShippedRoutes = sync.OnceValue(func() *domain.RoutingTable {
	routes, err := routingfile.Load(shippedRoutingFile)
	if err != nil {
		panic(err)
	}
	return routes
})

func shippedRoutes(t testing.TB) *domain.RoutingTable {
	t.Helper()
	return loadShippedRoutes()
}

func newEventsTestRouter(t testing.TB, store usecase.EventStreamStore) (*gin.Engine, *usecase.FanoutService) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	svc := usecase.NewFanoutService(store, shippedRoutes(t), usecase.CallerCheckOn)
	r.POST("/internal/events", NewEventsHandler(svc).Ingest)
	return r, svc
}

const workflowConsumerCert = `By=spiffe://cluster.local/ns/workflow-app/sa/workflow-stream-gateway;` +
	`Hash=abc;Subject="";URI=spiffe://cluster.local/ns/workflow-app/sa/workflow-events-consumer`

func postEvent(r *gin.Engine, body string) *httptest.ResponseRecorder {
	return postEventWithCert(r, body, workflowConsumerCert)
}

func postEventWithCert(r *gin.Engine, body, clientCert string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/internal/events", strings.NewReader(body))
	if clientCert != "" {
		req.Header.Set("X-Forwarded-Client-Cert", clientCert)
	}
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

const validTaskCreatedEnvelope = `{
	"id": "018f2c3e-6b1a-7c40-9e2f-000000000001",
	"type": "WorkflowTaskCreated",
	"source": "workflow-execution-svc",
	"tenant_id": "00000000-0000-4000-8000-0000000000a1",
	"time": "2026-08-21T10:00:00Z",
	"data": {"recipients": [{"kind": "user", "id": "00000000-0000-4000-8000-0000000000a1"}, {"kind": "user", "id": "00000000-0000-4000-8000-0000000000a2"}]}
}`

func TestEventsHandler_Ingest_KnownTypeSuccess(t *testing.T) {
	store := &fakeEventStore{}
	r, _ := newEventsTestRouter(t, store)

	rec := postEvent(r, validTaskCreatedEnvelope)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 2, store.appendCalls)
	assert.Equal(t, 1, store.broadcastCalls)
}

func TestEventsHandler_Ingest_UnknownTypeIgnoredWith200(t *testing.T) {
	store := &fakeEventStore{}
	r, _ := newEventsTestRouter(t, store)

	body := `{"id":"1","type":"user.deleted","source":"iam","time":"2026-08-21T10:00:00Z","data":{}}`
	rec := postEvent(r, body)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, store.appendCalls)
}

// errorReadCloser fails every Read — exercises Ingest's io.ReadAll error
// branch, which a normal strings.Reader body never hits.
type errorReadCloser struct{}

func (errorReadCloser) Read([]byte) (int, error) { return 0, errors.New("read failed") }
func (errorReadCloser) Close() error             { return nil }

func TestEventsHandler_Ingest_BodyReadFailureReturns400(t *testing.T) {
	store := &fakeEventStore{}
	r, _ := newEventsTestRouter(t, store)

	req := httptest.NewRequest(http.MethodPost, "/internal/events", nil)
	req.Body = errorReadCloser{}
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestEventsHandler_Ingest_MalformedBodyReturns400(t *testing.T) {
	store := &fakeEventStore{}
	r, _ := newEventsTestRouter(t, store)

	rec := postEvent(r, `not json`)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestEventsHandler_Ingest_MissingRequiredFieldReturns400(t *testing.T) {
	store := &fakeEventStore{}
	r, _ := newEventsTestRouter(t, store)

	// Missing "id".
	body := `{"type":"WorkflowTaskCreated","source":"workflow-execution-svc","time":"2026-08-21T10:00:00Z","data":{}}`
	rec := postEvent(r, body)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestEventsHandler_Ingest_KnownTypeInvalidPayloadReturns400(t *testing.T) {
	store := &fakeEventStore{}
	r, _ := newEventsTestRouter(t, store)

	body := `{
		"id": "1", "type": "WorkflowTaskCreated", "source": "workflow-execution-svc",
		"time": "2026-08-21T10:00:00Z", "data": {"recipients": [{"kind": "group", "id": "00000000-0000-4000-8000-0000000000f1"}]}
	}`
	rec := postEvent(r, body)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, 0, store.appendCalls)
}

func TestEventsHandler_Ingest_StoreFailureReturns502(t *testing.T) {
	store := &fakeEventStore{forceErr: errors.New("valkey unreachable")}
	r, _ := newEventsTestRouter(t, store)

	rec := postEvent(r, validTaskCreatedEnvelope)

	assert.Equal(t, http.StatusBadGateway, rec.Code)
}

func TestEventsHandler_Ingest_ReassignedFansOutToTwo(t *testing.T) {
	store := &fakeEventStore{}
	r, _ := newEventsTestRouter(t, store)

	body := `{
		"id": "1", "type": "WorkflowTaskReassigned", "source": "workflow-execution-svc",
		"time": "2026-08-21T10:00:00Z", "tenant_id": "00000000-0000-4000-8000-0000000000a1",
		"data": {"recipients": [{"kind": "user", "id": "00000000-0000-4000-8000-0000000000b1"}, {"kind": "user", "id": "00000000-0000-4000-8000-0000000000b2"}]}
	}`
	rec := postEvent(r, body)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 2, store.appendCalls)
	assert.Equal(t, 1, store.broadcastCalls)
}

// The event type is request-controlled text: as a metric label it would let a caller create a
// series per value.
func TestEventsHandler_Ingest_ATypeTheServiceDoesNotHandleIsLabelledUnknown(t *testing.T) {
	obstest.Init(t)
	const metric = "workflow_stream_gateway_internal_events_ingest_total"
	rawType := "Random" + rand.Text()
	store := &fakeEventStore{}
	r, _ := newEventsTestRouter(t, store)
	before := obstest.Sum(t, metric, map[string]string{"event_type": "unknown", "result": "ok"})

	rec := postEvent(r, `{"id":"1","type":"`+rawType+`","source":"iam","time":"2026-08-21T10:00:00Z","data":{}}`)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, before+1, obstest.Sum(t, metric, map[string]string{"event_type": "unknown", "result": "ok"}))
	assert.Zero(t, obstest.Sum(t, metric, map[string]string{"event_type": rawType}), "the raw type is not a label value")
}

func TestEventsHandler_Ingest_AHandledTypeKeepsItsOwnLabel(t *testing.T) {
	obstest.Init(t)
	const metric = "workflow_stream_gateway_internal_events_ingest_total"
	labels := map[string]string{"event_type": "workflow.WorkflowTaskCreated", "result": "ok"}
	r, _ := newEventsTestRouter(t, &fakeEventStore{})
	before := obstest.Sum(t, metric, labels)

	rec := postEvent(r, validTaskCreatedEnvelope)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, before+1, obstest.Sum(t, metric, labels))
}

func TestEventsHandler_Ingest_AWorkflowTypeFromAnotherSourceIsUnknown(t *testing.T) {
	store := &fakeEventStore{}
	r, _ := newEventsTestRouter(t, store)
	body := strings.Replace(validTaskCreatedEnvelope, `"source": "workflow-execution-svc"`, `"source": "tender-svc"`, 1)
	require.NotEqual(t, validTaskCreatedEnvelope, body)

	rec := postEvent(r, body)

	assert.Equal(t, http.StatusOK, rec.Code, "an unlisted (source, type) pair is acknowledged and ignored")
	assert.Zero(t, store.appendCalls+store.broadcastCalls+store.departmentCalls)
}

func TestEventsHandler_Ingest_ACallerOutsideTheSectionIsForbidden(t *testing.T) {
	obstest.Init(t)
	const metric = "workflow_stream_gateway_internal_auth_rejected_total"
	before := obstest.Sum(t, metric, map[string]string{"reason": "caller_not_allowed"})
	llmCert := `By=spiffe://cluster.local/ns/workflow-app/sa/workflow-stream-gateway;Hash=abc;Subject="";` +
		`URI=spiffe://cluster.local/ns/llm/sa/llm-retrieval-generation`
	for name, cert := range map[string]string{
		"another service's consumer":            llmCert,
		"no client certificate":                 "",
		"a forged element before the sidecar's": `URI=spiffe://cluster.local/ns/workflow-app/sa/workflow-events-consumer,` + llmCert,
	} {
		t.Run(name, func(t *testing.T) {
			store := &fakeEventStore{}
			r, _ := newEventsTestRouter(t, store)

			rec := postEventWithCert(r, validTaskCreatedEnvelope, cert)

			assert.Equal(t, http.StatusForbidden, rec.Code)
			assert.Contains(t, rec.Body.String(), `"code":"forbidden"`)
			assert.Equal(t, 0, store.appendCalls+store.broadcastCalls+store.departmentCalls)
		})
	}
	after := obstest.Sum(t, metric, map[string]string{"reason": "caller_not_allowed"})
	assert.Equal(t, before+3, after)
}

func TestCallerFromClientCert(t *testing.T) {
	for name, tc := range map[string]struct{ header, want string }{
		"one element":           {`By=spiffe://cluster.local/ns/a/sa/b;Hash=h;URI=spiffe://cluster.local/ns/workflow-app/sa/consumer`, "workflow-app/consumer"},
		"the last element wins": {`URI=spiffe://cluster.local/ns/x/sa/forged,By=spiffe://td/ns/a/sa/b;URI=spiffe://td/ns/llm/sa/real`, "llm/real"},
		"a quoted subject with a comma and semicolon": {`By=spiffe://td/ns/a/sa/b;Subject="CN=a,O=b;c";URI=spiffe://td/ns/n/sa/s`, "n/s"},
		"a quoted URI":                {`By=x;URI="spiffe://td/ns/n/sa/s"`, "n/s"},
		"empty":                       {"", ""},
		"no URI":                      {`By=spiffe://td/ns/a/sa/b;Hash=h`, ""},
		"not spiffe":                  {`URI=https://td/ns/n/sa/s`, ""},
		"another path":                {`URI=spiffe://td/ns/n/sa/s/extra`, ""},
		"missing service account":     {`URI=spiffe://td/ns/n`, ""},
		"forged earlier, none at end": {`URI=spiffe://td/ns/n/sa/s,By=spiffe://td/ns/a/sa/b`, ""},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, callerFromClientCert(tc.header))
		})
	}
}
