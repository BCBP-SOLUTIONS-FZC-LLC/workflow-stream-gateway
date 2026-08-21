package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

type fakeEventStore struct {
	appendCalls int
	forceErr    error
}

func (f *fakeEventStore) AppendEvent(_ context.Context, _, _, _ string, _ []byte) error {
	f.appendCalls++
	if f.forceErr != nil {
		return f.forceErr
	}
	return nil
}

func newEventsTestRouter(store usecase.EventStreamStore) (*gin.Engine, *usecase.FanoutService) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	svc := usecase.NewFanoutService(store)
	r.POST("/internal/events", NewEventsHandler(svc).Ingest)
	return r, svc
}

func postEvent(r *gin.Engine, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/internal/events", strings.NewReader(body))
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

const validTaskCreatedEnvelope = `{
	"id": "018f2c3e-6b1a-7c40-9e2f-000000000001",
	"type": "workflow.task.created",
	"source": "workflow-execution-svc",
	"tenant_id": "tenant-a",
	"time": "2026-08-21T10:00:00Z",
	"data": {"assignee_user_ids": ["u1", "u2"]}
}`

func TestEventsHandler_Ingest_KnownTypeSuccess(t *testing.T) {
	store := &fakeEventStore{}
	r, _ := newEventsTestRouter(store)

	rec := postEvent(r, validTaskCreatedEnvelope)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 2, store.appendCalls)
}

func TestEventsHandler_Ingest_UnknownTypeIgnoredWith200(t *testing.T) {
	store := &fakeEventStore{}
	r, _ := newEventsTestRouter(store)

	body := `{"id":"1","type":"user.deleted","source":"iam","time":"2026-08-21T10:00:00Z","data":{}}`
	rec := postEvent(r, body)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, store.appendCalls)
}

func TestEventsHandler_Ingest_MalformedBodyReturns400(t *testing.T) {
	store := &fakeEventStore{}
	r, _ := newEventsTestRouter(store)

	rec := postEvent(r, `not json`)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestEventsHandler_Ingest_MissingRequiredFieldReturns400(t *testing.T) {
	store := &fakeEventStore{}
	r, _ := newEventsTestRouter(store)

	// Missing "id".
	body := `{"type":"workflow.task.created","source":"workflow-execution-svc","time":"2026-08-21T10:00:00Z","data":{}}`
	rec := postEvent(r, body)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestEventsHandler_Ingest_KnownTypeInvalidPayloadReturns400(t *testing.T) {
	store := &fakeEventStore{}
	r, _ := newEventsTestRouter(store)

	body := `{
		"id": "1", "type": "workflow.task.created", "source": "workflow-execution-svc",
		"time": "2026-08-21T10:00:00Z", "data": {"assignee_user_ids": "not-an-array"}
	}`
	rec := postEvent(r, body)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, 0, store.appendCalls)
}

func TestEventsHandler_Ingest_StoreFailureReturns502(t *testing.T) {
	store := &fakeEventStore{forceErr: errors.New("valkey unreachable")}
	r, _ := newEventsTestRouter(store)

	rec := postEvent(r, validTaskCreatedEnvelope)

	assert.Equal(t, http.StatusBadGateway, rec.Code)
}

func TestEventsHandler_Ingest_ReassignedFansOutToTwo(t *testing.T) {
	store := &fakeEventStore{}
	r, _ := newEventsTestRouter(store)

	body := `{
		"id": "1", "type": "workflow.task.reassigned", "source": "workflow-execution-svc",
		"time": "2026-08-21T10:00:00Z", "tenant_id": "tenant-a",
		"data": {"old_user_id": "old-1", "new_user_id": "new-1", "assignee_user_ids": ["new-1"]}
	}`
	rec := postEvent(r, body)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 2, store.appendCalls)
}
