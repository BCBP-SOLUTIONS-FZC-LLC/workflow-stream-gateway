package usecase

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
)

// serveRefreshFailLimiter fails every Refresh and counts the calls.
type serveRefreshFailLimiter struct {
	fakeLimiter
	refreshErr   error
	refreshCalls int
}

func (l *serveRefreshFailLimiter) Refresh(_ context.Context, _, _ string, _ time.Duration) error {
	l.refreshCalls++
	return l.refreshErr
}

// serveControlFailSink fails every control frame write.
type serveControlFailSink struct {
	fakeSink
	controlErr error
}

func (s *serveControlFailSink) WriteControlFrame(string, []byte) error {
	return s.controlErr
}

func TestStreamService_Serve_LimiterRefreshFailure_StreamContinues(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
	}{
		{name: "refresh error is ignored and later frames are still delivered"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := &fakeStreamStore{
				readNewSeq: []readNewResult{
					{frames: nil},
					{frames: []domain.Frame{{ID: "200-0", EventType: "WorkflowTaskCreated", Data: []byte(`{}`), SourceKey: testPersonalKey}}},
				},
			}
			limiter := &serveRefreshFailLimiter{refreshErr: errors.New("redis down")}
			svc := newTestStreamService(store, limiter)
			sink := &fakeSink{}

			err := svc.Serve(context.Background(), testTicket(), "", sink)

			assert.ErrorIs(t, err, errStopTest, "loop ends only through the store, not the refresh failure")
			assert.Equal(t, 2, limiter.refreshCalls)
			assert.Equal(t, 1, sink.heartbeats)
			require.Len(t, sink.frames, 1)
			assert.Equal(t, "200-0", decodeCursor(sink.frames[0].ID)[testPersonalKey])
		})
	}
}

func TestStreamService_Serve_ResyncControlFrameWriteFailure_ReturnsSinkError(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
	}{
		{name: "sink error is returned before the live loop"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := &fakeStreamStore{oldestID: "500-0", oldestExists: true}
			svc := newTestStreamService(store, &fakeLimiter{})
			sinkErr := errors.New("control write failed")
			sink := &serveControlFailSink{controlErr: sinkErr}

			err := svc.Serve(context.Background(), testTicket(), compoundID(testPersonalKey, "100-0"), sink)

			assert.ErrorIs(t, err, sinkErr)
			assert.NotErrorIs(t, err, errStopTest)
			assert.Empty(t, store.readNewCalls, "live loop never started")
		})
	}
}
