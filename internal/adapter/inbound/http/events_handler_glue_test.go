package http

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingEventStore struct {
	envelopes [][]byte
}

func (s *recordingEventStore) AppendEvent(_ context.Context, _, _, _ string, raw []byte) error {
	s.envelopes = append(s.envelopes, raw)
	return nil
}

func (s *recordingEventStore) AppendBroadcastEvent(_ context.Context, _, _ string, raw []byte) error {
	s.envelopes = append(s.envelopes, raw)
	return nil
}

func (s *recordingEventStore) AppendDepartmentEvent(_ context.Context, _, _, _ string, raw []byte) error {
	s.envelopes = append(s.envelopes, raw)
	return nil
}

const (
	glueSchemaVersionID = "5d3a1b9e-8c4f-4a7e-9b2d-1f0e6c7a8b90"
	taskCreatedData     = `{"task_id":"t-1","recipients":[{"kind":"user","id":"00000000-0000-4000-8000-0000000000a1"}]}`
)

func glueFrame(version, compression byte, body string) []byte {
	header := make([]byte, 18)
	header[0] = version
	header[1] = compression
	return append(header, body...)
}

func glueEnvelope(t *testing.T, data any) string {
	t.Helper()
	env := map[string]any{
		"id":         "018f2c3e-6b1a-7c40-9e2f-000000000001",
		"type":       "WorkflowTaskCreated",
		"source":     "workflow-execution-svc",
		"tenant_id":  "00000000-0000-4000-8000-0000000000a1",
		"time":       "2026-08-21T10:00:00Z",
		"dataschema": glueSchemaVersionID,
		"data":       data,
	}
	body, err := json.Marshal(env)
	require.NoError(t, err)
	return string(body)
}

func b64(frame []byte) string {
	return base64.StdEncoding.EncodeToString(frame)
}

func TestEventsHandler_Ingest_GlueFramedDataIsDecodedBeforeFanOut(t *testing.T) {
	store := &recordingEventStore{}
	r, _ := newEventsTestRouter(t, store)

	rec := postEvent(r, glueEnvelope(t, b64(glueFrame(0x03, 0x00, taskCreatedData))))

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Len(t, store.envelopes, 2)
	for _, raw := range store.envelopes {
		var stored map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(raw, &stored))
		assert.JSONEq(t, taskCreatedData, string(stored["data"]))
		assert.NotContains(t, stored, "dataschema")
		assert.JSONEq(t, `"WorkflowTaskCreated"`, string(stored["type"]))
		assert.JSONEq(t, `"00000000-0000-4000-8000-0000000000a1"`, string(stored["tenant_id"]))
	}
}

func TestEventsHandler_Ingest_PlainEnvelopeIsStoredUnchanged(t *testing.T) {
	store := &recordingEventStore{}
	r, _ := newEventsTestRouter(t, store)

	rec := postEvent(r, validTaskCreatedEnvelope)

	require.Equal(t, http.StatusOK, rec.Code)
	require.NotEmpty(t, store.envelopes)
	for _, raw := range store.envelopes {
		assert.Equal(t, validTaskCreatedEnvelope, string(raw))
	}
}

func TestEventsHandler_Ingest_UndecodableGlueDataReturns400(t *testing.T) {
	cases := []struct {
		name string
		data any
	}{
		{name: "data is an object, not a string", data: map[string]any{"recipients": []any{}}},
		{name: "data is not base64", data: "not base64!"},
		{name: "frame shorter than the header", data: b64([]byte{0x03, 0x00})},
		{name: "wrong header version", data: b64(glueFrame(0x02, 0x00, taskCreatedData))},
		{name: "compressed frame", data: b64(glueFrame(0x03, 0x05, taskCreatedData))},
		{name: "body after the header is not JSON", data: b64(glueFrame(0x03, 0x00, "avro"))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &recordingEventStore{}
			r, _ := newEventsTestRouter(t, store)

			rec := postEvent(r, glueEnvelope(t, tc.data))

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "invalid-payload")
			assert.Empty(t, store.envelopes)
		})
	}
}
