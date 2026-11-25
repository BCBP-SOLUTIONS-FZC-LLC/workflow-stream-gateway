package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability/obstest"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

const gwBStreamPath = "/api/v1/stream"

func gwBStreamWiring() *gwBWiring {
	w := newGwBWiring()
	w.tickets.tickets["tok"] = gwBTicketFor(testTenantID, testUserID)
	return w
}

// nothingOpened asserts that a refused connection never reached the stream or the open counter.
func (w *gwBWiring) nothingOpened(t *testing.T, openBefore float64) {
	t.Helper()
	assert.Zero(t, w.stream.latestCalls+w.stream.readCalls, "Serve was never called")
	assert.Empty(t, w.limiter.releases, "a refused connection has no slot to release")
	assert.Equal(t, openBefore, obstest.Sum(t, gwBOpenMetric, nil), "the open-connections gauge is untouched")
}

func TestGwB_Stream_ANoTicketRequestIsAnUnauthorizedProblem(t *testing.T) {
	for name, target := range map[string]string{
		"no ticket parameter": gwBStreamPath,
		"empty ticket":        gwBStreamPath + "?ticket=",
	} {
		t.Run(name, func(t *testing.T) {
			obstest.Init(t)
			w := gwBStreamWiring()
			r := w.router()
			openBefore := obstest.Sum(t, gwBOpenMetric, nil)
			rejectedBefore := obstest.Sum(t, gwBRejectedMetric, nil)

			rec := w.serve(r, httptest.NewRequest(http.MethodGet, target, nil))

			p := gwBAssertProblem(t, rec, http.StatusUnauthorized, "unauthorized", "missing ticket query parameter")
			assert.Equal(t, gwBStreamPath, p.Instance)
			assert.Equal(t, rejectedBefore, obstest.Sum(t, gwBRejectedMetric, nil), "no rejected-connection series moves")
			assert.Empty(t, w.tickets.gets, "no ticket lookup is made without a ticket")
			assert.Empty(t, w.limiter.acquires)
			w.nothingOpened(t, openBefore)
		})
	}
}

func TestGwB_Stream_AnUnknownTicketIsAnUnauthorizedProblemAndCountedAsRejected(t *testing.T) {
	obstest.Init(t)
	w := gwBStreamWiring()
	r := w.router()
	labels := map[string]string{"reason": "invalid_ticket"}
	beforeOwn := obstest.Sum(t, gwBRejectedMetric, labels)
	beforeAll := obstest.Sum(t, gwBRejectedMetric, nil)
	openBefore := obstest.Sum(t, gwBOpenMetric, nil)

	rec := w.serve(r, httptest.NewRequest(http.MethodGet, gwBStreamPath+"?ticket=unknown-ticket", nil))

	p := gwBAssertProblem(t, rec, http.StatusUnauthorized, "unauthorized", "ticket missing or expired")
	assert.Equal(t, gwBStreamPath, p.Instance)
	assert.Equal(t, []string{"unknown-ticket"}, w.tickets.gets)
	assert.Empty(t, w.limiter.acquires, "no connection slot is taken for an invalid ticket")
	assert.Equal(t, beforeOwn+1, obstest.Sum(t, gwBRejectedMetric, labels))
	assert.Equal(t, beforeAll+1, obstest.Sum(t, gwBRejectedMetric, nil), "only the invalid_ticket series moved")
	w.nothingOpened(t, openBefore)
}

// Any ticket-store error that is not "not found" fails closed with a 503, whatever it wraps.
func TestGwB_Stream_ATicketLookupFailureIsAnUnavailableProblemAndCountedAsRejected(t *testing.T) {
	cases := map[string]error{
		"the store is unreachable":      errors.New("valkey unreachable"),
		"the stored ticket is unusable": fmt.Errorf("unmarshal ticket: %w", errors.New("invalid character 'x' looking for beginning of value")),
	}
	for name, lookupErr := range cases {
		t.Run(name, func(t *testing.T) {
			obstest.Init(t)
			w := gwBStreamWiring()
			w.tickets.getErr = lookupErr
			r := w.router()
			labels := map[string]string{"reason": "valkey_unavailable"}
			beforeOwn := obstest.Sum(t, gwBRejectedMetric, labels)
			beforeAll := obstest.Sum(t, gwBRejectedMetric, nil)
			openBefore := obstest.Sum(t, gwBOpenMetric, nil)

			rec := w.serve(r, httptest.NewRequest(http.MethodGet, gwBStreamPath+"?ticket=tok", nil))

			gwBAssertProblem(t, rec, http.StatusServiceUnavailable, "unavailable", "ticket store unavailable")
			assert.Equal(t, []string{"tok"}, w.tickets.gets)
			assert.Empty(t, w.limiter.acquires)
			assert.Equal(t, beforeOwn+1, obstest.Sum(t, gwBRejectedMetric, labels))
			assert.Equal(t, beforeAll+1, obstest.Sum(t, gwBRejectedMetric, nil))
			w.nothingOpened(t, openBefore)
		})
	}
}

func TestGwB_Stream_AFailingConnectionStoreIsAnUnavailableProblemAndNoStreamOpens(t *testing.T) {
	obstest.Init(t)
	w := gwBStreamWiring()
	w.limiter.acquireErr = errors.New("valkey acquire: connection refused")
	r := w.router()
	labels := map[string]string{"reason": "valkey_unavailable"}
	beforeOwn := obstest.Sum(t, gwBRejectedMetric, labels)
	beforeAll := obstest.Sum(t, gwBRejectedMetric, nil)
	openBefore := obstest.Sum(t, gwBOpenMetric, nil)

	rec := w.serve(r, httptest.NewRequest(http.MethodGet, gwBStreamPath+"?ticket=tok", nil))

	gwBAssertProblem(t, rec, http.StatusServiceUnavailable, "unavailable", "connection store unavailable")
	assert.Equal(t, []gwBAcquire{{tenant: testTenantID, user: testUserID, cap: gwBConnCap, ttl: gwBConnTTL}}, w.limiter.acquires)
	assert.Equal(t, beforeOwn+1, obstest.Sum(t, gwBRejectedMetric, labels))
	assert.Equal(t, beforeAll+1, obstest.Sum(t, gwBRejectedMetric, nil))
	w.nothingOpened(t, openBefore)
}

func TestGwB_Stream_AUserAtTheCapIsAConnectionCapProblemWithoutRetryAfter(t *testing.T) {
	obstest.Init(t)
	w := gwBStreamWiring()
	w.limiter.grant = false
	r := w.router()
	labels := map[string]string{"reason": "cap_exceeded"}
	beforeOwn := obstest.Sum(t, gwBRejectedMetric, labels)
	beforeAll := obstest.Sum(t, gwBRejectedMetric, nil)
	openBefore := obstest.Sum(t, gwBOpenMetric, nil)

	rec := w.serve(r, httptest.NewRequest(http.MethodGet, gwBStreamPath+"?ticket=tok", nil))

	gwBAssertProblem(t, rec, http.StatusTooManyRequests, "connection-cap-exceeded", "concurrent connection cap exceeded")
	assert.Empty(t, rec.Header().Get("Retry-After"))
	assert.Equal(t, []gwBAcquire{{tenant: testTenantID, user: testUserID, cap: gwBConnCap, ttl: gwBConnTTL}}, w.limiter.acquires)
	assert.Equal(t, beforeOwn+1, obstest.Sum(t, gwBRejectedMetric, labels))
	assert.Equal(t, beforeAll+1, obstest.Sum(t, gwBRejectedMetric, nil))
	w.nothingOpened(t, openBefore)
}

// An open stream answers 200 with the four SSE headers, counts as open while it runs, and gives
// its slot back (same tenant and user) when it ends.
func TestGwB_Stream_AnOpenStreamHasTheSSEHeadersAndIsCountedWhileItRuns(t *testing.T) {
	obstest.Init(t)
	w := gwBStreamWiring()
	openBefore := obstest.Sum(t, gwBOpenMetric, nil)
	var openWhileRunning float64
	var releasesWhileRunning int
	w.stream.readNew = func(context.Context, []usecase.StreamPosition) ([]domain.Frame, error) {
		openWhileRunning = obstest.Sum(t, gwBOpenMetric, nil)
		releasesWhileRunning = len(w.limiter.releases)
		return nil, errGwBStop
	}
	r := w.router()

	rec := w.serve(r, httptest.NewRequest(http.MethodGet, gwBStreamPath+"?ticket=tok", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "text/event-stream; charset=utf-8", rec.Header().Get("Content-Type"))
	assert.Equal(t, "no-cache, no-transform", rec.Header().Get("Cache-Control"))
	assert.Equal(t, "keep-alive", rec.Header().Get("Connection"))
	assert.Equal(t, "no", rec.Header().Get("X-Accel-Buffering"))
	assert.Equal(t, openBefore+1, openWhileRunning, "the gauge counts the stream while it runs")
	assert.Zero(t, releasesWhileRunning, "the slot is held while it runs")
	assert.Equal(t, openBefore, obstest.Sum(t, gwBOpenMetric, nil), "the gauge drops when the stream ends")
	assert.Equal(t, []gwBRelease{{tenant: testTenantID, user: testUserID}}, w.limiter.releases)
	assert.Equal(t, []gwBAcquire{{tenant: testTenantID, user: testUserID, cap: gwBConnCap, ttl: gwBConnTTL}}, w.limiter.acquires)
}

// Shutdown ends an open stream: the live read is cut short, the handler returns a normal 200, the
// open gauge drops and the slot is released for the same tenant and user.
func TestGwB_Stream_ShutdownEndsAnOpenStreamAndReleasesItsSlot(t *testing.T) {
	obstest.Init(t)
	stopping, beginShutdown := context.WithCancel(context.Background())
	t.Cleanup(beginShutdown)
	w := gwBStreamWiring()
	w.stopping = stopping
	openBefore := obstest.Sum(t, gwBOpenMetric, nil)
	var openAtShutdown float64
	var releasesAtShutdown int
	var cancelledBefore bool
	w.stream.readNew = func(ctx context.Context, _ []usecase.StreamPosition) ([]domain.Frame, error) {
		cancelledBefore = ctx.Err() != nil
		openAtShutdown = obstest.Sum(t, gwBOpenMetric, nil)
		releasesAtShutdown = len(w.limiter.releases)
		beginShutdown()
		<-ctx.Done()
		return nil, ctx.Err()
	}
	r := w.router()

	rec := w.serve(r, httptest.NewRequest(http.MethodGet, gwBStreamPath+"?ticket=tok", nil))

	assert.False(t, cancelledBefore, "the stream was running before shutdown began")
	assert.Equal(t, openBefore+1, openAtShutdown)
	assert.Zero(t, releasesAtShutdown)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "keep-alive", "the stream ended without another loop")
	assert.Equal(t, 1, w.stream.readCalls)
	assert.Equal(t, openBefore, obstest.Sum(t, gwBOpenMetric, nil), "the gauge dropped")
	assert.Equal(t, []gwBRelease{{tenant: testTenantID, user: testUserID}}, w.limiter.releases, "the slot is released")
	assert.Zero(t, w.limiter.refreshes, "no refresh after the cut-short read")
}

type gwBSink struct{ frames, controls, heartbeats int }

func (s *gwBSink) WriteFrame(string, string, []byte) error { s.frames++; return nil }
func (s *gwBSink) WriteControlFrame(string, []byte) error  { s.controls++; return nil }
func (s *gwBSink) WriteHeartbeat() error                   { s.heartbeats++; return nil }

// Serve returns nil when its context ends (client gone or server shutting down), and the read
// error itself when the context is still live.
func TestGwB_StreamService_ServeEndsWithNilOnlyWhenItsContextEnded(t *testing.T) {
	cases := []struct {
		name          string
		cancelFirst   bool
		readNew       func(ctx context.Context, cancel context.CancelFunc) ([]domain.Frame, error)
		wantErrIs     error
		wantReadCalls int
	}{
		{"cancelled before the live tail starts", true, nil, nil, 0},
		{"cancelled while the live read waits", false, func(ctx context.Context, cancel context.CancelFunc) ([]domain.Frame, error) {
			cancel()
			<-ctx.Done()
			return nil, ctx.Err()
		}, nil, 1},
		{"the read fails on a live context", false, func(context.Context, context.CancelFunc) ([]domain.Frame, error) {
			return nil, errGwBStop
		}, errGwBStop, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			if tc.cancelFirst {
				cancel()
			}
			store := &gwBStreamStore{}
			if tc.readNew != nil {
				store.readNew = func(ctx2 context.Context, _ []usecase.StreamPosition) ([]domain.Frame, error) {
					return tc.readNew(ctx2, cancel)
				}
			}
			limiter := &gwBLimiter{grant: true}
			svc := usecase.NewStreamService(usecase.NewTicketService(newGwBTicketStore(), time.Minute, 10, time.Minute), store, limiter, gwBConnCap, gwBConnTTL, time.Millisecond)
			sink := &gwBSink{}

			err := svc.Serve(ctx, gwBTicketFor(testTenantID, testUserID), "", sink)

			if tc.wantErrIs == nil {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, tc.wantErrIs)
			}
			assert.Equal(t, tc.wantReadCalls, store.readCalls)
			assert.Equal(t, gwBSink{}, *sink, "nothing is written once the stream ends")
			assert.Zero(t, limiter.refreshes)
		})
	}
}

// gwBRecordingWriter logs every write and flush the sink makes, in order.
type gwBRecordingWriter struct {
	gin.ResponseWriter
	log     *[]string
	failing bool
}

func (w *gwBRecordingWriter) Write(b []byte) (int, error) {
	if w.failing {
		return 0, errors.New("write failed")
	}
	*w.log = append(*w.log, "write:"+string(b))
	return w.ResponseWriter.Write(b)
}

func (w *gwBRecordingWriter) Flush() {
	*w.log = append(*w.log, "flush")
	w.ResponseWriter.Flush()
}

func gwBNewSink(failing bool) (ginFrameSink, *[]string, *httptest.ResponseRecorder) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	var log []string
	return ginFrameSink{w: &gwBRecordingWriter{ResponseWriter: c.Writer, log: &log, failing: failing}}, &log, rec
}

// Each frame is one write of the exact wire bytes, flushed straight after, so a client sees it
// at once.
func TestGwB_GinFrameSink_EachFrameIsOneExactWriteFollowedByAFlush(t *testing.T) {
	cases := []struct {
		name  string
		write func(s ginFrameSink) error
		want  string
	}{
		{"data frame", func(s ginFrameSink) error {
			return s.WriteFrame("k=1-0", "workflow.WorkflowTaskCreated", []byte(`{"x":1}`))
		}, "id: k=1-0\nevent: workflow.WorkflowTaskCreated\ndata: {\"x\":1}\n\n"},
		{"control frame has no id line", func(s ginFrameSink) error {
			return s.WriteControlFrame("resync-required", []byte(`{"reason":"backfill_window_exceeded"}`))
		}, "event: resync-required\ndata: {\"reason\":\"backfill_window_exceeded\"}\n\n"},
		{"heartbeat is a comment", func(s ginFrameSink) error { return s.WriteHeartbeat() }, ": keep-alive\n\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sink, log, rec := gwBNewSink(false)

			require.NoError(t, tc.write(sink))

			assert.Equal(t, []string{"write:" + tc.want, "flush"}, *log)
			assert.Equal(t, tc.want, rec.Body.String())
			assert.True(t, rec.Flushed)
		})
	}
}

func TestGwB_GinFrameSink_AFailedWriteIsNotFlushed(t *testing.T) {
	cases := map[string]func(s ginFrameSink) error{
		"data frame":    func(s ginFrameSink) error { return s.WriteFrame("k=1-0", "x", []byte(`{}`)) },
		"control frame": func(s ginFrameSink) error { return s.WriteControlFrame("resync-required", []byte(`{}`)) },
		"heartbeat":     func(s ginFrameSink) error { return s.WriteHeartbeat() },
	}
	for name, write := range cases {
		t.Run(name, func(t *testing.T) {
			sink, log, rec := gwBNewSink(true)

			err := write(sink)

			require.Error(t, err)
			assert.ErrorContains(t, err, "write failed")
			assert.Empty(t, *log, "no flush after a failed write")
			assert.False(t, rec.Flushed)
		})
	}
}
