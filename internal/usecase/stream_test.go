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

// fakeTargetState overrides fakeStreamStore's default backfill response
// for one specific target (keyed by fakeStreamStore.KeyFor) — lets a test
// give different targets of the same connection different outcomes
// (e.g. personal replays cleanly while a department has gone stale).
type fakeTargetState struct {
	oldestID     string
	oldestExists bool
	oldestErr    error
	rangeFrames  []domain.Frame
	rangeErr     error
}

type fakeStreamStore struct {
	// Default backfill response, used for any target with no perTarget
	// entry — keeps every single-target (personal-only) test below
	// simple.
	oldestID     string
	oldestExists bool
	oldestErr    error
	rangeFrames  []domain.Frame
	rangeErr     error

	perTarget map[string]fakeTargetState

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

// KeyFor mirrors the real adapter's key-naming scheme (streamstore.go's
// keyFor) closely enough for tests: same shape, real key strings don't
// matter here, only that it's stable and this fake is internally
// consistent with itself.
func (f *fakeStreamStore) KeyFor(t StreamTarget) string {
	switch {
	case t.UserID != "":
		return "sse:events:" + t.TenantID + ":" + t.UserID
	case t.DepartmentID != "":
		return "sse:events:broadcast:" + t.TenantID + ":" + t.DepartmentID
	default:
		return "sse:events:broadcast:" + t.TenantID
	}
}

func (f *fakeStreamStore) OldestEntryID(_ context.Context, target StreamTarget) (string, bool, error) {
	if st, ok := f.perTarget[f.KeyFor(target)]; ok {
		return st.oldestID, st.oldestExists, st.oldestErr
	}
	return f.oldestID, f.oldestExists, f.oldestErr
}

func (f *fakeStreamStore) RangeSince(_ context.Context, target StreamTarget, _ string) ([]domain.Frame, error) {
	key := f.KeyFor(target)
	if st, ok := f.perTarget[key]; ok {
		return tagSourceKey(key, st.rangeFrames), st.rangeErr
	}
	return tagSourceKey(key, f.rangeFrames), f.rangeErr
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

// tagSourceKey stamps every frame with the key it was "read from" — the
// real adapter does this too (streamstore.go's toFramesFrom); Serve
// relies on Frame.SourceKey to update the right slot of a connection's
// compound resume cursor.
func tagSourceKey(key string, frames []domain.Frame) []domain.Frame {
	if frames == nil {
		return nil
	}
	tagged := make([]domain.Frame, len(frames))
	for i, f := range frames {
		f.SourceKey = key
		tagged[i] = f
	}
	return tagged
}

type fakeLimiter struct {
	refreshed  int
	acquireOK  bool
	acquireErr error
	released   bool
	releaseErr error
}

func (f *fakeLimiter) TryAcquire(_ context.Context, _, _ string, _ int, _ time.Duration) (bool, error) {
	return f.acquireOK, f.acquireErr
}

func (f *fakeLimiter) Refresh(_ context.Context, _, _ string, _ time.Duration) error {
	f.refreshed++
	return nil
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

// testPersonalKey is the key fakeStreamStore.KeyFor produces for
// testTicket()'s personal target — every backfill/reconnect test below
// needs it to build a valid compound Last-Event-ID.
const testPersonalKey = "sse:events:tenant-a:user-1"

// compoundID is a small test helper: encode a one-target compound cursor,
// the same shape a real reconnecting client would echo back.
func compoundID(key, id string) string {
	return encodeCursor(map[string]string{key: id})
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

// --- backfill (single target) --------------------------------------------

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
			{ID: "150-0", EventType: "WorkflowTaskCompleted", Data: []byte(`{}`)},
			{ID: "160-0", EventType: "WorkflowTaskCompleted", Data: []byte(`{}`)},
		},
	}
	svc := newTestStreamService(store, &fakeLimiter{})
	sink := &fakeSink{}

	err := svc.Serve(context.Background(), testTicket(), compoundID(testPersonalKey, "140-0"), sink)

	assert.ErrorIs(t, err, errStopTest)
	require.Len(t, sink.frames, 2)
	assert.Equal(t, "150-0", decodeCursor(sink.frames[0].ID)[testPersonalKey])
	assert.Equal(t, "160-0", decodeCursor(sink.frames[1].ID)[testPersonalKey])
	assert.Empty(t, sink.control)
}

func TestStreamService_Serve_BackfillBeyondWindowEmitsResyncRequired(t *testing.T) {
	store := &fakeStreamStore{
		oldestID:     "500-0", // oldest retained entry is newer than the client's last-seen ID
		oldestExists: true,
	}
	svc := newTestStreamService(store, &fakeLimiter{})
	sink := &fakeSink{}

	err := svc.Serve(context.Background(), testTicket(), compoundID(testPersonalKey, "100-0"), sink)

	assert.ErrorIs(t, err, errStopTest)
	assert.Empty(t, sink.frames)
	require.Len(t, sink.control, 1)
	assert.Equal(t, "resync-required", sink.control[0].EventType)
}

func TestStreamService_Serve_BackfillStreamKeyExpiredEmitsResyncRequired(t *testing.T) {
	store := &fakeStreamStore{oldestExists: false}
	svc := newTestStreamService(store, &fakeLimiter{})
	sink := &fakeSink{}

	err := svc.Serve(context.Background(), testTicket(), compoundID(testPersonalKey, "100-0"), sink)

	assert.ErrorIs(t, err, errStopTest)
	require.Len(t, sink.control, 1)
	assert.Equal(t, "resync-required", sink.control[0].EventType)
}

func TestStreamService_Serve_BackfillStoreErrorPropagates(t *testing.T) {
	store := &fakeStreamStore{oldestErr: errors.New("valkey unreachable")}
	svc := newTestStreamService(store, &fakeLimiter{})
	sink := &fakeSink{}

	err := svc.Serve(context.Background(), testTicket(), compoundID(testPersonalKey, "100-0"), sink)

	require.Error(t, err)
	assert.NotErrorIs(t, err, errStopTest)
}

// A bare, non-compound Last-Event-ID (e.g. a client that somehow echoed
// back an old-format single stream ID) decodes to no valid pairs — same
// as an empty string, no backfill for any target, not an error.
func TestStreamService_Serve_MalformedLastEventIDSkipsBackfill(t *testing.T) {
	store := &fakeStreamStore{
		oldestID:     "100-0",
		oldestExists: true,
		rangeFrames:  []domain.Frame{{ID: "150-0", EventType: "x", Data: []byte(`{}`)}},
	}
	svc := newTestStreamService(store, &fakeLimiter{})
	sink := &fakeSink{}

	err := svc.Serve(context.Background(), testTicket(), "140-0", sink)

	assert.ErrorIs(t, err, errStopTest)
	assert.Empty(t, sink.frames)
	assert.Empty(t, sink.control)
}

// --- backfill (multiple targets) ------------------------------------------

func TestStreamService_Serve_BackfillCoversTenantWideBroadcastToo(t *testing.T) {
	const broadcastKey = "sse:events:broadcast:tenant-a"
	store := &fakeStreamStore{
		perTarget: map[string]fakeTargetState{
			testPersonalKey: {
				oldestID: "100-0", oldestExists: true,
				rangeFrames: []domain.Frame{{ID: "150-0", EventType: "WorkflowTaskCompleted", Data: []byte(`{}`)}},
			},
			broadcastKey: {
				oldestID: "100-0", oldestExists: true,
				rangeFrames: []domain.Frame{{ID: "155-0", EventType: "WorkflowInstancePaused", Data: []byte(`{}`)}},
			},
		},
	}
	svc := newTestStreamService(store, &fakeLimiter{})
	sink := &fakeSink{}

	lastEventID := encodeCursor(map[string]string{testPersonalKey: "140-0", broadcastKey: "140-0"})
	ticket := domain.Ticket{TenantID: "tenant-a", UserID: "user-1", TenantRoles: []string{"tenant_admin"}}
	err := svc.Serve(context.Background(), ticket, lastEventID, sink)

	assert.ErrorIs(t, err, errStopTest)
	require.Len(t, sink.frames, 2)
	// Personal replays first (streamTargets' own ordering), then broadcast.
	assert.Equal(t, "150-0", decodeCursor(sink.frames[0].ID)[testPersonalKey])
	assert.Equal(t, "155-0", decodeCursor(sink.frames[1].ID)[broadcastKey])
	// The compound id after both frames still carries the personal
	// cursor forward even though only the broadcast target advanced on
	// the second frame — otherwise a subsequent reconnect would silently
	// drop the personal stream's resume position.
	assert.Equal(t, "150-0", decodeCursor(sink.frames[1].ID)[testPersonalKey])
	assert.Empty(t, sink.control)
}

func TestStreamService_Serve_BackfillOnlyAppliesToTargetsWithACursorEntry(t *testing.T) {
	// A ticket newly granted a department mid-connection has no prior
	// cursor entry for that department's key — that target gets no
	// backfill (not an error, not a resync), matching "no lastEventID ->
	// replay nothing," just scoped to this one target.
	store := &fakeStreamStore{
		perTarget: map[string]fakeTargetState{
			testPersonalKey: {
				oldestID: "100-0", oldestExists: true,
				rangeFrames: []domain.Frame{{ID: "150-0", EventType: "x", Data: []byte(`{}`)}},
			},
		},
	}
	svc := newTestStreamService(store, &fakeLimiter{})
	sink := &fakeSink{}

	ticket := domain.Ticket{
		TenantID:    "tenant-a",
		UserID:      "user-1",
		Departments: []domain.DepartmentRole{{DepartmentID: "dept-a", Role: "approver"}},
	}
	err := svc.Serve(context.Background(), ticket, compoundID(testPersonalKey, "140-0"), sink)

	assert.ErrorIs(t, err, errStopTest)
	require.Len(t, sink.frames, 1)
	assert.Equal(t, "150-0", decodeCursor(sink.frames[0].ID)[testPersonalKey])
	assert.Empty(t, sink.control)
}

func TestStreamService_Serve_AnyTargetBeyondWindowResyncsWholeConnection(t *testing.T) {
	const broadcastKey = "sse:events:broadcast:tenant-a"
	store := &fakeStreamStore{
		perTarget: map[string]fakeTargetState{
			testPersonalKey: {oldestID: "100-0", oldestExists: true}, // fine
			broadcastKey:    {oldestID: "500-0", oldestExists: true}, // window exceeded
		},
	}
	svc := newTestStreamService(store, &fakeLimiter{})
	sink := &fakeSink{}

	lastEventID := encodeCursor(map[string]string{testPersonalKey: "140-0", broadcastKey: "140-0"})
	ticket := domain.Ticket{TenantID: "tenant-a", UserID: "user-1", TenantRoles: []string{"tenant_admin"}}
	err := svc.Serve(context.Background(), ticket, lastEventID, sink)

	assert.ErrorIs(t, err, errStopTest)
	assert.Empty(t, sink.frames)
	require.Len(t, sink.control, 1)
	assert.Equal(t, "resync-required", sink.control[0].EventType)
}

// --- cursor propagation through live-tail --------------------------------

func TestStreamService_Serve_LiveTailCursorCarriesPersonalPositionForward(t *testing.T) {
	const broadcastKey = "sse:events:broadcast:tenant-a"
	store := &fakeStreamStore{
		perTarget: map[string]fakeTargetState{
			testPersonalKey: {oldestID: "100-0", oldestExists: true,
				rangeFrames: []domain.Frame{{ID: "150-0", EventType: "x", Data: []byte(`{}`)}}},
			broadcastKey: {oldestExists: false},
		},
		readNewSeq: []readNewResult{
			{frames: []domain.Frame{{ID: "300-0", EventType: "WorkflowInstancePaused", Data: []byte(`{}`), SourceKey: broadcastKey}}},
		},
	}
	svc := newTestStreamService(store, &fakeLimiter{})
	sink := &fakeSink{}

	lastEventID := encodeCursor(map[string]string{testPersonalKey: "140-0"})
	ticket := domain.Ticket{TenantID: "tenant-a", UserID: "user-1", TenantRoles: []string{"tenant_admin"}}
	err := svc.Serve(context.Background(), ticket, lastEventID, sink)

	assert.ErrorIs(t, err, errStopTest)
	// backfill only had a cursor entry for the personal target (broadcast
	// was absent from the incoming Last-Event-ID, e.g. a ticket that just
	// gained tenant-wide eligibility) -> no window-exceeded check ran for
	// it, so no resync.
	assert.Empty(t, sink.control)
	require.Len(t, sink.frames, 2)
	assert.Equal(t, "150-0", decodeCursor(sink.frames[0].ID)[testPersonalKey])
	// The live-tailed broadcast frame's emitted compound id still carries
	// the personal stream's position forward.
	got := decodeCursor(sink.frames[1].ID)
	assert.Equal(t, "300-0", got[broadcastKey])
	assert.Equal(t, "150-0", got[testPersonalKey])
}

// --- live-tail loop ------------------------------------------------------

func TestStreamService_Serve_LiveTailDeliversFrames(t *testing.T) {
	store := &fakeStreamStore{
		readNewSeq: []readNewResult{
			{frames: nil}, // timeout -> heartbeat
			{frames: []domain.Frame{{ID: "200-0", EventType: "WorkflowTaskCreated", Data: []byte(`{}`), SourceKey: testPersonalKey}}},
		},
	}
	svc := newTestStreamService(store, &fakeLimiter{})
	sink := &fakeSink{}

	err := svc.Serve(context.Background(), testTicket(), "", sink)

	assert.ErrorIs(t, err, errStopTest)
	assert.Equal(t, 1, sink.heartbeats)
	require.Len(t, sink.frames, 1)
	assert.Equal(t, "200-0", decodeCursor(sink.frames[0].ID)[testPersonalKey])
}

func TestStreamService_Serve_RefreshesTheConnectionCountWhileOpen(t *testing.T) {
	store := &fakeStreamStore{
		readNewSeq: []readNewResult{{frames: nil}, {frames: nil}, {frames: nil}},
	}
	limiter := &fakeLimiter{}
	svc := newTestStreamService(store, limiter)

	_ = svc.Serve(context.Background(), testTicket(), "", &fakeSink{})

	assert.Equal(t, 3, limiter.refreshed, "every wake-up of a live connection keeps its counter from expiring")
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
			{frames: []domain.Frame{{ID: "1-0", EventType: "x", Data: []byte(`{}`), SourceKey: testPersonalKey}}},
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

// --- cursor encode/decode --------------------------------------------------

func TestCursor_RoundTrip(t *testing.T) {
	in := map[string]string{testPersonalKey: "150-0", "sse:events:broadcast:tenant-a": "160-1"}
	out := decodeCursor(encodeCursor(in))
	assert.Equal(t, in, out)
}

func TestDecodeCursor_Empty(t *testing.T) {
	assert.Nil(t, decodeCursor(""))
}

func TestDecodeCursor_MalformedPairsSkipped(t *testing.T) {
	got := decodeCursor("no-equals-sign|" + testPersonalKey + "=150-0|=empty-key|trailing=")
	assert.Equal(t, map[string]string{testPersonalKey: "150-0"}, got)
}

func TestEncodeCursor_Empty(t *testing.T) {
	assert.Equal(t, "", encodeCursor(nil))
	assert.Equal(t, "", encodeCursor(map[string]string{}))
}
