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
	readNewSeq   []readNewResult
	readNewIdx   int
	readNewCalls [][]StreamTarget // records the targets passed on every call
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

func (f *fakeStreamStore) ReadNew(_ context.Context, targets []StreamTarget, _ time.Duration) ([]domain.Frame, error) {
	f.readNewCalls = append(f.readNewCalls, targets)
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

func testTicket() domain.Ticket {
	return domain.Ticket{TenantID: "tenant-a", UserID: "user-1"}
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

	err := svc.Serve(context.Background(), testTicket(), "", sink)

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

	err := svc.Serve(context.Background(), testTicket(), "140-0", sink)

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

	err := svc.Serve(context.Background(), testTicket(), "100-0", sink)

	assert.ErrorIs(t, err, errStopTest)
	assert.Empty(t, sink.frames)
	require.Len(t, sink.control, 1)
	assert.Equal(t, "resync-required", sink.control[0].EventType)
}

func TestStreamService_Serve_BackfillStreamKeyExpiredEmitsResyncRequired(t *testing.T) {
	store := &fakeStreamStore{oldestExists: false}
	svc := newTestStreamService(store, &fakeLimiter{})
	sink := &fakeSink{}

	err := svc.Serve(context.Background(), testTicket(), "100-0", sink)

	assert.ErrorIs(t, err, errStopTest)
	require.Len(t, sink.control, 1)
	assert.Equal(t, "resync-required", sink.control[0].EventType)
}

func TestStreamService_Serve_BackfillStoreErrorPropagates(t *testing.T) {
	store := &fakeStreamStore{oldestErr: errors.New("valkey unreachable")}
	svc := newTestStreamService(store, &fakeLimiter{})
	sink := &fakeSink{}

	err := svc.Serve(context.Background(), testTicket(), "100-0", sink)

	require.Error(t, err)
	assert.NotErrorIs(t, err, errStopTest)
}

// backfill is always personal-only, regardless of broadcast eligibility —
// this ticket qualifies for the tenant-wide tier, but OldestEntryID/
// RangeSince (both stubbed via fakeStreamStore's plain tenantID/userID
// params) are never given anything but the personal identity, since the
// fake doesn't even have a way to be called with anything else. The real
// assertion of interest is that Serve doesn't panic/misbehave when an
// eligible ticket reconnects with a lastEventID.
func TestStreamService_Serve_BackfillIgnoresBroadcastEligibility(t *testing.T) {
	store := &fakeStreamStore{
		oldestID:     "100-0",
		oldestExists: true,
		rangeFrames:  []domain.Frame{{ID: "150-0", EventType: "workflow.instance.paused", Data: []byte(`{}`)}},
	}
	svc := newTestStreamService(store, &fakeLimiter{})
	sink := &fakeSink{}

	ticket := domain.Ticket{TenantID: "tenant-a", UserID: "user-1", TenantRoles: []string{"tenant_admin"}}
	err := svc.Serve(context.Background(), ticket, "140-0", sink)

	assert.ErrorIs(t, err, errStopTest)
	require.Len(t, sink.frames, 1)
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

	err := svc.Serve(context.Background(), testTicket(), "", sink)

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

	err := svc.Serve(ctx, testTicket(), "", sink)
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

	err := svc.Serve(context.Background(), testTicket(), "", sink)

	require.Error(t, err)
	assert.NotErrorIs(t, err, errStopTest)
}

// --- eligibility / streamTargets -----------------------------------------

func TestIsTenantWideEligible(t *testing.T) {
	assert.True(t, isTenantWideEligible([]string{"tenant_admin"}))
	assert.True(t, isTenantWideEligible([]string{"tenant_owner"}))
	assert.True(t, isTenantWideEligible([]string{"member", "tenant_owner"}))
	assert.False(t, isTenantWideEligible([]string{"member"}))
	assert.False(t, isTenantWideEligible(nil))
	// Not a real role anywhere on this platform — must not accidentally
	// qualify.
	assert.False(t, isTenantWideEligible([]string{"admin"}))
}

func TestStreamTargets_PersonalOnly(t *testing.T) {
	ticket := domain.Ticket{TenantID: "tenant-a", UserID: "user-1"}
	targets := streamTargets(ticket)

	require.Len(t, targets, 1)
	assert.Equal(t, StreamTarget{TenantID: "tenant-a", UserID: "user-1"}, targets[0])
}

func TestStreamTargets_TenantWideEligibleSkipsDepartments(t *testing.T) {
	ticket := domain.Ticket{
		TenantID:    "tenant-a",
		UserID:      "user-1",
		TenantRoles: []string{"tenant_admin"},
		Departments: []domain.DepartmentRole{{DepartmentID: "dept-a", Role: "approver"}},
	}
	targets := streamTargets(ticket)

	require.Len(t, targets, 2)
	assert.Equal(t, StreamTarget{TenantID: "tenant-a", UserID: "user-1"}, targets[0])
	assert.Equal(t, StreamTarget{TenantID: "tenant-a"}, targets[1])
}

func TestStreamTargets_MultipleDepartmentsDeduplicated(t *testing.T) {
	ticket := domain.Ticket{
		TenantID: "tenant-a",
		UserID:   "user-1",
		Departments: []domain.DepartmentRole{
			{DepartmentID: "dept-a", Role: "approver"},
			{DepartmentID: "dept-b", Role: "reviewer"},
			{DepartmentID: "dept-a", Role: "member"}, // duplicate department, different role
		},
	}
	targets := streamTargets(ticket)

	require.Len(t, targets, 3)
	assert.Equal(t, StreamTarget{TenantID: "tenant-a", UserID: "user-1"}, targets[0])
	assert.ElementsMatch(t, []StreamTarget{
		{TenantID: "tenant-a", DepartmentID: "dept-a"},
		{TenantID: "tenant-a", DepartmentID: "dept-b"},
	}, targets[1:])
}

func TestStreamService_Serve_PassesComputedTargetsToReadNew(t *testing.T) {
	store := &fakeStreamStore{}
	svc := newTestStreamService(store, &fakeLimiter{})
	sink := &fakeSink{}

	ticket := domain.Ticket{
		TenantID:    "tenant-a",
		UserID:      "user-1",
		Departments: []domain.DepartmentRole{{DepartmentID: "dept-a", Role: "approver"}},
	}
	err := svc.Serve(context.Background(), ticket, "", sink)

	assert.ErrorIs(t, err, errStopTest)
	require.Len(t, store.readNewCalls, 1)
	assert.ElementsMatch(t, []StreamTarget{
		{TenantID: "tenant-a", UserID: "user-1"},
		{TenantID: "tenant-a", DepartmentID: "dept-a"},
	}, store.readNewCalls[0])
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
