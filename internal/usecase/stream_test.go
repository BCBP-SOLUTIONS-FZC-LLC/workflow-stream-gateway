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

// --- fakes -------------------------------------------------------------

type fakeStreamStore struct {
	oldestID     string
	oldestExists bool
	oldestErr    error

	rangeFrames []domain.Frame
	rangeErr    error

	// readNewSeq is consumed one entry per ReadNew call; each entry is
	// either a frame batch or an error. When exhausted, ReadNew returns
	// errStopTest so tests terminate deterministically without real time.
	readNewSeq []readNewResult
	readNewIdx int
}

type readNewResult struct {
	frames []domain.Frame
	err    error
}

var errStopTest = errors.New("test: stop loop")

func (f *fakeStreamStore) OldestEntryID(_ context.Context, _, _ string) (string, bool, error) {
	return f.oldestID, f.oldestExists, f.oldestErr
}

func (f *fakeStreamStore) RangeSince(_ context.Context, _, _, _ string) ([]domain.Frame, error) {
	return f.rangeFrames, f.rangeErr
}

func (f *fakeStreamStore) ReadNew(_ context.Context, _, _ string, _ time.Duration) ([]domain.Frame, error) {
	if f.readNewIdx >= len(f.readNewSeq) {
		return nil, errStopTest
	}
	r := f.readNewSeq[f.readNewIdx]
	f.readNewIdx++
	return r.frames, r.err
}

type fakeLimiter struct {
	acquireOK  bool
	acquireErr error
	released   bool
	releaseErr error
}

func (f *fakeLimiter) TryAcquire(_ context.Context, _, _ string, _ int, _ time.Duration) (bool, error) {
	return f.acquireOK, f.acquireErr
}

func (f *fakeLimiter) Release(_ context.Context, _, _ string) error {
	f.released = true
	return f.releaseErr
}

type fakeSink struct {
	frames       []domain.Frame
	control      []domain.Frame
	heartbeats   int
	failOnFrameN int // 1-indexed WriteFrame call to fail on; 0 = never
	frameCalls   int
}

func (s *fakeSink) WriteFrame(id, eventType string, data []byte) error {
	s.frameCalls++
	if s.failOnFrameN != 0 && s.frameCalls == s.failOnFrameN {
		return errors.New("sink write failed")
	}
	s.frames = append(s.frames, domain.Frame{ID: id, EventType: eventType, Data: data})
	return nil
}

func (s *fakeSink) WriteControlFrame(eventType string, data []byte) error {
	s.control = append(s.control, domain.Frame{EventType: eventType, Data: data})
	return nil
}

func (s *fakeSink) WriteHeartbeat() error {
	s.heartbeats++
	return nil
}

func newTestStreamService(store StreamStore, limiter ConnectionLimiter) *StreamService {
	tickets := NewTicketService(newFakeStore(), time.Minute, 10, time.Minute)
	return NewStreamService(tickets, store, limiter, 5, 2*time.Minute, time.Millisecond)
}

// --- AcquireConnection / ReleaseConnection ------------------------------

func TestStreamService_AcquireConnection(t *testing.T) {
	limiter := &fakeLimiter{acquireOK: true}
	svc := newTestStreamService(&fakeStreamStore{}, limiter)

	ok, err := svc.AcquireConnection(context.Background(), "tenant-a", "user-1")
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestStreamService_AcquireConnection_CapExceeded(t *testing.T) {
	limiter := &fakeLimiter{acquireOK: false}
	svc := newTestStreamService(&fakeStreamStore{}, limiter)

	ok, err := svc.AcquireConnection(context.Background(), "tenant-a", "user-1")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestStreamService_ReleaseConnection(t *testing.T) {
	limiter := &fakeLimiter{}
	svc := newTestStreamService(&fakeStreamStore{}, limiter)

	require.NoError(t, svc.ReleaseConnection(context.Background(), "tenant-a", "user-1"))
	assert.True(t, limiter.released)
}

// --- backfill ------------------------------------------------------------

func TestStreamService_Serve_FreshConnectionNoBackfill(t *testing.T) {
	store := &fakeStreamStore{readNewSeq: []readNewResult{}}
	svc := newTestStreamService(store, &fakeLimiter{})
	sink := &fakeSink{}

	err := svc.Serve(context.Background(), "tenant-a", "user-1", "", sink)

	assert.ErrorIs(t, err, errStopTest)
	assert.Empty(t, sink.frames)
	assert.Empty(t, sink.control)
}

func TestStreamService_Serve_BackfillWithinWindow(t *testing.T) {
	store := &fakeStreamStore{
		oldestID:     "100-0",
		oldestExists: true,
		rangeFrames: []domain.Frame{
			{ID: "150-0", EventType: "workflow.task.completed", Data: []byte(`{}`)},
			{ID: "160-0", EventType: "workflow.task.completed", Data: []byte(`{}`)},
		},
	}
	svc := newTestStreamService(store, &fakeLimiter{})
	sink := &fakeSink{}

	err := svc.Serve(context.Background(), "tenant-a", "user-1", "140-0", sink)

	assert.ErrorIs(t, err, errStopTest)
	require.Len(t, sink.frames, 2)
	assert.Equal(t, "150-0", sink.frames[0].ID)
	assert.Empty(t, sink.control)
}

func TestStreamService_Serve_BackfillBeyondWindowEmitsResyncRequired(t *testing.T) {
	store := &fakeStreamStore{
		oldestID:     "500-0", // oldest retained entry is newer than the client's last-seen ID
		oldestExists: true,
	}
	svc := newTestStreamService(store, &fakeLimiter{})
	sink := &fakeSink{}

	err := svc.Serve(context.Background(), "tenant-a", "user-1", "100-0", sink)

	assert.ErrorIs(t, err, errStopTest)
	assert.Empty(t, sink.frames)
	require.Len(t, sink.control, 1)
	assert.Equal(t, "resync-required", sink.control[0].EventType)
}

func TestStreamService_Serve_BackfillStreamKeyExpiredEmitsResyncRequired(t *testing.T) {
	store := &fakeStreamStore{oldestExists: false}
	svc := newTestStreamService(store, &fakeLimiter{})
	sink := &fakeSink{}

	err := svc.Serve(context.Background(), "tenant-a", "user-1", "100-0", sink)

	assert.ErrorIs(t, err, errStopTest)
	require.Len(t, sink.control, 1)
	assert.Equal(t, "resync-required", sink.control[0].EventType)
}

func TestStreamService_Serve_BackfillStoreErrorPropagates(t *testing.T) {
	store := &fakeStreamStore{oldestErr: errors.New("valkey unreachable")}
	svc := newTestStreamService(store, &fakeLimiter{})
	sink := &fakeSink{}

	err := svc.Serve(context.Background(), "tenant-a", "user-1", "100-0", sink)

	require.Error(t, err)
	assert.NotErrorIs(t, err, errStopTest)
}

// --- live-tail loop ------------------------------------------------------

func TestStreamService_Serve_LiveTailDeliversFrames(t *testing.T) {
	store := &fakeStreamStore{
		readNewSeq: []readNewResult{
			{frames: nil}, // timeout -> heartbeat
			{frames: []domain.Frame{{ID: "200-0", EventType: "workflow.task.created", Data: []byte(`{}`)}}},
		},
	}
	svc := newTestStreamService(store, &fakeLimiter{})
	sink := &fakeSink{}

	err := svc.Serve(context.Background(), "tenant-a", "user-1", "", sink)

	assert.ErrorIs(t, err, errStopTest)
	assert.Equal(t, 1, sink.heartbeats)
	require.Len(t, sink.frames, 1)
	assert.Equal(t, "200-0", sink.frames[0].ID)
}

func TestStreamService_Serve_ContextCancellationEndsCleanly(t *testing.T) {
	store := &fakeStreamStore{}
	svc := newTestStreamService(store, &fakeLimiter{})
	sink := &fakeSink{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := svc.Serve(ctx, "tenant-a", "user-1", "", sink)
	assert.NoError(t, err)
}

func TestStreamService_Serve_SinkWriteFailureAborts(t *testing.T) {
	store := &fakeStreamStore{
		readNewSeq: []readNewResult{
			{frames: []domain.Frame{{ID: "1-0", EventType: "x", Data: []byte(`{}`)}}},
		},
	}
	svc := newTestStreamService(store, &fakeLimiter{})
	sink := &fakeSink{failOnFrameN: 1}

	err := svc.Serve(context.Background(), "tenant-a", "user-1", "", sink)

	require.Error(t, err)
	assert.NotErrorIs(t, err, errStopTest)
}

// --- compareStreamIDs / parseStreamID ------------------------------------

func TestCompareStreamIDs(t *testing.T) {
	assert.Equal(t, 0, compareStreamIDs("100-0", "100-0"))
	assert.Equal(t, -1, compareStreamIDs("100-0", "200-0"))
	assert.Equal(t, 1, compareStreamIDs("200-0", "100-0"))
	assert.Equal(t, -1, compareStreamIDs("100-0", "100-1"))
	assert.Equal(t, 1, compareStreamIDs("100-1", "100-0"))
	// A malformed ID parses to (0,0) — treated as "very old."
	assert.Equal(t, 1, compareStreamIDs("100-0", "not-an-id"))
}
