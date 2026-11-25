package http

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability/obstest"
)

const gwBEventsPath = "/api/v1/internal/events"

// Each outcome of an ingest increments events_ingest_total under its own event_type and result,
// and no other series.
func TestGwB_EventsIngest_EachOutcomeIsCountedUnderItsOwnLabels(t *testing.T) {
	obstest.Init(t)
	cases := []struct {
		name       string
		body       string
		storeErr   error
		wantStatus int
		wantLabels map[string]string
	}{
		{"accepted", gwBValidEnvelope, nil, http.StatusOK, map[string]string{"event_type": gwBCreatedType, "result": "ok"}},
		{"payload the type does not accept", gwBInvalidPayloadEnvelope, nil, http.StatusBadRequest, map[string]string{"event_type": gwBCreatedType, "result": "bad_payload"}},
		{"store failure", gwBValidEnvelope, errors.New("valkey unreachable"), http.StatusBadGateway, map[string]string{"event_type": gwBCreatedType, "result": "error"}},
		{"envelope that is not JSON", `not json`, nil, http.StatusBadRequest, map[string]string{"event_type": "unknown", "result": "bad_payload"}},
		{"envelope without an id", `{"type":"WorkflowTaskCreated","source":"workflow-execution-svc","time":"2026-08-21T10:00:00Z","data":{}}`, nil, http.StatusBadRequest, map[string]string{"event_type": "unknown", "result": "bad_payload"}},
		{"type no service routes", `{"id":"1","type":"user.deleted","source":"iam","time":"2026-08-21T10:00:00Z","data":{}}`, nil, http.StatusOK, map[string]string{"event_type": "unknown", "result": "ok"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newGwBWiring()
			w.events.err = tc.storeErr
			r := w.router()
			beforeAll := obstest.Sum(t, gwBIngestMetric, nil)
			beforeOwn := obstest.Sum(t, gwBIngestMetric, tc.wantLabels)

			rec := w.serve(r, gwBSystemPost(gwBEventsPath, tc.body))

			require.Equal(t, tc.wantStatus, rec.Code, rec.Body.String())
			assert.Equal(t, beforeOwn+1, obstest.Sum(t, gwBIngestMetric, tc.wantLabels))
			assert.Equal(t, beforeAll+1, obstest.Sum(t, gwBIngestMetric, nil), "exactly one series moved, by one")
		})
	}
}

// A caller outside the section is refused before any ingest is counted: only the auth-rejected
// counter moves.
func TestGwB_EventsIngest_AForbiddenCallerIsNotCountedAsAnIngest(t *testing.T) {
	obstest.Init(t)
	w := newGwBWiring()
	r := w.router()
	req := gwBSystemPost(gwBEventsPath, gwBValidEnvelope)
	req.Header.Del(clientCertHeader)
	beforeIngest := obstest.Sum(t, gwBIngestMetric, nil)
	beforeAuth := obstest.Sum(t, gwBAuthMetric, map[string]string{"reason": "caller_not_allowed"})

	rec := w.serve(r, req)

	gwBAssertProblem(t, rec, http.StatusForbidden, "forbidden", "the caller may not publish events for this source")
	assert.Equal(t, beforeIngest, obstest.Sum(t, gwBIngestMetric, nil))
	assert.Equal(t, beforeAuth+1, obstest.Sum(t, gwBAuthMetric, map[string]string{"reason": "caller_not_allowed"}))
	assert.Zero(t, w.events.total())
}

// With the collectors not registered (a unit test, or a boot that skipped them) every outcome
// still answers as usual.
func TestGwB_EventsIngest_WithoutCollectorsEveryOutcomeStillAnswers(t *testing.T) {
	obstest.Init(t)
	savedIngest, savedAuth := observability.EventsIngestTotal, observability.InternalAuthRejectedTotal
	observability.EventsIngestTotal, observability.InternalAuthRejectedTotal = nil, nil
	t.Cleanup(func() {
		observability.EventsIngestTotal, observability.InternalAuthRejectedTotal = savedIngest, savedAuth
	})
	cases := []struct {
		name       string
		body       string
		noCert     bool
		storeErr   error
		wantStatus int
		wantStored int
	}{
		{"accepted", gwBValidEnvelope, false, nil, http.StatusOK, 3},
		{"bad payload", gwBInvalidPayloadEnvelope, false, nil, http.StatusBadRequest, 0},
		{"store failure", gwBValidEnvelope, false, errors.New("valkey unreachable"), http.StatusBadGateway, 0},
		{"malformed envelope", `not json`, false, nil, http.StatusBadRequest, 0},
		{"unknown type", `{"id":"1","type":"user.deleted","source":"iam","time":"2026-08-21T10:00:00Z","data":{}}`, false, nil, http.StatusOK, 0},
		{"forbidden caller", gwBValidEnvelope, true, nil, http.StatusForbidden, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newGwBWiring()
			w.events.err = tc.storeErr
			r := w.router()
			req := gwBSystemPost(gwBEventsPath, tc.body)
			if tc.noCert {
				req.Header.Del(clientCertHeader)
			}

			var rec *httptest.ResponseRecorder
			require.NotPanics(t, func() { rec = w.serve(r, req) })

			assert.Equal(t, tc.wantStatus, rec.Code, rec.Body.String())
			assert.Equal(t, tc.wantStored, w.events.total(), "the fan-out ran exactly as with collectors")
		})
	}
}

// A body that is not a usable envelope is a 400 malformed-envelope problem, counted as a bad
// payload of unknown type, and nothing is stored.
func TestGwB_EventsIngest_AnUnusableEnvelopeIsAMalformedEnvelopeProblem(t *testing.T) {
	obstest.Init(t)
	cases := []struct {
		name       string
		body       string
		wantDetail func(t *testing.T, detail string)
	}{
		{"not JSON", `not json`, func(t *testing.T, d string) { assert.NotEmpty(t, d) }},
		{"a JSON array", `[]`, func(t *testing.T, d string) { assert.NotEmpty(t, d) }},
		{"no id", `{"type":"WorkflowTaskCreated","source":"workflow-execution-svc","time":"2026-08-21T10:00:00Z"}`, func(t *testing.T, d string) { assert.Equal(t, events.ErrEnvelopeIDRequired.Error(), d) }},
		{"no type", `{"id":"1","source":"workflow-execution-svc","time":"2026-08-21T10:00:00Z"}`, func(t *testing.T, d string) { assert.Equal(t, events.ErrEnvelopeTypeRequired.Error(), d) }},
		{"no source", `{"id":"1","type":"WorkflowTaskCreated","time":"2026-08-21T10:00:00Z"}`, func(t *testing.T, d string) { assert.Equal(t, events.ErrEnvelopeSourceRequired.Error(), d) }},
		{"no time", `{"id":"1","type":"WorkflowTaskCreated","source":"workflow-execution-svc"}`, func(t *testing.T, d string) { assert.Contains(t, d, "time") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newGwBWiring()
			r := w.router()
			labels := map[string]string{"event_type": "unknown", "result": "bad_payload"}
			before := obstest.Sum(t, gwBIngestMetric, labels)

			rec := w.serve(r, gwBSystemPost(gwBEventsPath, tc.body))

			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			p := gwBAssertProblemShape(t, rec, http.StatusBadRequest, "malformed-envelope")
			tc.wantDetail(t, p.Detail)
			assert.Equal(t, gwBEventsPath, p.Instance)
			assert.Equal(t, before+1, obstest.Sum(t, gwBIngestMetric, labels))
			assert.Zero(t, w.events.total())
		})
	}
}

// failingBody fails every read with an error that is not a size error (a client that aborted).
type gwBFailingBody struct{}

func (gwBFailingBody) Read([]byte) (int, error) { return 0, errors.New("client aborted") }
func (gwBFailingBody) Close() error             { return nil }

func TestGwB_EventsIngest_ABodyThatCannotBeReadIsAMalformedEnvelopeProblem(t *testing.T) {
	obstest.Init(t)
	w := newGwBWiring()
	r := w.router()
	req := gwBSystemPost(gwBEventsPath, "")
	req.Body = gwBFailingBody{}
	labels := map[string]string{"event_type": "unknown", "result": "bad_payload"}
	before := obstest.Sum(t, gwBIngestMetric, labels)

	rec := w.serve(r, req)

	p := gwBAssertProblem(t, rec, http.StatusBadRequest, "malformed-envelope", "unable to read request body")
	assert.Equal(t, gwBEventsPath, p.Instance)
	assert.Equal(t, before+1, obstest.Sum(t, gwBIngestMetric, labels))
	assert.Zero(t, w.events.total())
}

// The 10 MiB limit: a byte over is a 413 payload-too-large problem counted as a bad payload;
// exactly at the limit the body is read in full and judged as an envelope.
func TestGwB_EventsIngest_TheBodyLimitIsExactlyTenMiB(t *testing.T) {
	obstest.Init(t)
	const limit = 10 << 20
	cases := []struct {
		name       string
		size       int
		wantStatus int
		wantCode   string
		wantDetail string
	}{
		{"one byte over", limit + 1, http.StatusRequestEntityTooLarge, "payload-too-large", "request body exceeds the 10 MiB limit"},
		{"far over", limit + 1<<20, http.StatusRequestEntityTooLarge, "payload-too-large", "request body exceeds the 10 MiB limit"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newGwBWiring()
			r := w.router()
			labels := map[string]string{"event_type": "unknown", "result": "bad_payload"}
			before := obstest.Sum(t, gwBIngestMetric, labels)

			rec := w.serve(r, gwBSystemPost(gwBEventsPath, strings.Repeat("x", tc.size)))

			p := gwBAssertProblem(t, rec, tc.wantStatus, tc.wantCode, tc.wantDetail)
			assert.Equal(t, gwBEventsPath, p.Instance)
			assert.Equal(t, before+1, obstest.Sum(t, gwBIngestMetric, labels))
			assert.Zero(t, w.events.total())
		})
	}

	t.Run("exactly at the limit is read and judged as an envelope", func(t *testing.T) {
		w := newGwBWiring()
		r := w.router()

		rec := w.serve(r, gwBSystemPost(gwBEventsPath, strings.Repeat("x", limit)))

		gwBAssertProblemShape(t, rec, http.StatusBadRequest, "malformed-envelope")
	})
}
