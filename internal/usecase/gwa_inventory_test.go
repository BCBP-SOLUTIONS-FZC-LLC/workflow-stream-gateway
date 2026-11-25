package usecase

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
)

const (
	gwATenant       = "00000000-0000-4000-8000-00000000a001"
	gwAOtherTenant  = "00000000-0000-4000-8000-00000000a002"
	gwAUser         = "00000000-0000-4000-8000-00000000b001"
	gwAOtherUser    = "00000000-0000-4000-8000-00000000b002"
	gwADeptOne      = "00000000-0000-4000-8000-00000000c001"
	gwADeptTwo      = "00000000-0000-4000-8000-00000000c002"
	gwAHeartbeat    = 7 * time.Millisecond
	gwAConnTTL      = 3 * time.Minute
	gwAWorkflowSrc  = "gwa-workflow-svc"
	gwAWorkflowCall = "gwa-ns/gwa-workflow-caller"
	gwAOtherSrc     = "gwa-other-svc"
	gwAOtherCall    = "gwa-ns/gwa-other-caller"
)

var (
	gwAPersonal  = StreamTarget{TenantID: gwATenant, UserID: gwAUser}
	gwABroadcast = StreamTarget{TenantID: gwATenant}
	gwADeptA     = StreamTarget{TenantID: gwATenant, DepartmentID: gwADeptOne}
	gwADeptB     = StreamTarget{TenantID: gwATenant, DepartmentID: gwADeptTwo}

	errGwAStop = errors.New("gwA: stop the live loop")
	errGwABoom = errors.New("gwA: injected failure")
)

func gwAKey(t StreamTarget) string {
	switch {
	case t.UserID != "":
		return "sse:events:" + t.TenantID + ":" + t.UserID
	case t.DepartmentID != "":
		return "sse:events:broadcast:" + t.TenantID + ":" + t.DepartmentID
	default:
		return "sse:events:broadcast:" + t.TenantID
	}
}

// gwALog is the shared, ordered record of every store and sink call.
type gwALog struct{ entries []string }

func (l *gwALog) add(format string, args ...any) {
	l.entries = append(l.entries, fmt.Sprintf(format, args...))
}

func (l *gwALog) has(entry string) bool {
	for _, e := range l.entries {
		if e == entry {
			return true
		}
	}
	return false
}

func (l *gwALog) count(prefix string) int {
	n := 0
	for _, e := range l.entries {
		if len(e) >= len(prefix) && e[:len(prefix)] == prefix {
			n++
		}
	}
	return n
}

func (l *gwALog) indexOf(entry string) int {
	for i, e := range l.entries {
		if e == entry {
			return i
		}
	}
	return -1
}

type gwAOldest struct {
	id     string
	exists bool
	err    error
}

type gwARange struct {
	frames []domain.Frame
	err    error
}

type gwAReadStep struct {
	frames []domain.Frame
	err    error
	// run is called inside ReadNew before it returns (used to cancel the context).
	run func()
}

// gwAStore is a hand-rolled StreamStore that records every call in order.
type gwAStore struct {
	log *gwALog

	latest    map[string]string
	latestErr map[string]error
	oldest    map[string]gwAOldest
	ranges    map[string]gwARange

	rangeAfter map[string]string
	reads      [][]StreamPosition
	blocks     []time.Duration
	readSteps  []gwAReadStep
}

func newGwAStore(log *gwALog) *gwAStore {
	return &gwAStore{
		log:        log,
		latest:     map[string]string{},
		latestErr:  map[string]error{},
		oldest:     map[string]gwAOldest{},
		ranges:     map[string]gwARange{},
		rangeAfter: map[string]string{},
	}
}

func (s *gwAStore) KeyFor(t StreamTarget) string { return gwAKey(t) }

func (s *gwAStore) OldestEntryID(_ context.Context, t StreamTarget) (string, bool, error) {
	key := gwAKey(t)
	s.log.add("oldest:%s", key)
	o := s.oldest[key]
	return o.id, o.exists, o.err
}

func (s *gwAStore) LatestEntryID(_ context.Context, t StreamTarget) (string, error) {
	key := gwAKey(t)
	s.log.add("latest:%s", key)
	if err := s.latestErr[key]; err != nil {
		return "", err
	}
	if id, ok := s.latest[key]; ok {
		return id, nil
	}
	return NoEntryID, nil
}

func (s *gwAStore) RangeSince(_ context.Context, t StreamTarget, afterID string) ([]domain.Frame, error) {
	key := gwAKey(t)
	s.log.add("range:%s", key)
	s.rangeAfter[key] = afterID
	r := s.ranges[key]
	if r.err != nil {
		return nil, r.err
	}
	out := make([]domain.Frame, len(r.frames))
	for i, f := range r.frames {
		f.SourceKey = key
		out[i] = f
	}
	return out, nil
}

func (s *gwAStore) ReadNew(_ context.Context, positions []StreamPosition, block time.Duration) ([]domain.Frame, error) {
	s.log.add("read")
	s.reads = append(s.reads, append([]StreamPosition(nil), positions...))
	s.blocks = append(s.blocks, block)
	if len(s.reads) > len(s.readSteps) {
		return nil, errGwAStop
	}
	step := s.readSteps[len(s.reads)-1]
	if step.run != nil {
		step.run()
	}
	return step.frames, step.err
}

type gwAFrame struct {
	id, eventType string
	data          []byte
}

type gwASink struct {
	log          *gwALog
	frames       []gwAFrame
	control      []gwAFrame
	heartbeats   int
	frameErrOn   int // 1-indexed WriteFrame call that fails, 0 never
	controlErr   error
	heartbeatErr error
	frameCalls   int
}

func (s *gwASink) WriteFrame(id, eventType string, data []byte) error {
	s.frameCalls++
	s.log.add("frame")
	if s.frameErrOn != 0 && s.frameCalls == s.frameErrOn {
		return errGwABoom
	}
	s.frames = append(s.frames, gwAFrame{id, eventType, data})
	return nil
}

func (s *gwASink) WriteControlFrame(eventType string, data []byte) error {
	s.log.add("control")
	if s.controlErr != nil {
		return s.controlErr
	}
	s.control = append(s.control, gwAFrame{eventType: eventType, data: data})
	return nil
}

func (s *gwASink) WriteHeartbeat() error {
	s.log.add("heartbeat")
	if s.heartbeatErr != nil {
		return s.heartbeatErr
	}
	s.heartbeats++
	return nil
}

type gwARefresh struct {
	tenantID, userID string
	ttl              time.Duration
}

type gwALimiter struct {
	refreshes []gwARefresh
}

func (l *gwALimiter) TryAcquire(context.Context, string, string, int, time.Duration) (bool, error) {
	return true, nil
}

func (l *gwALimiter) Release(context.Context, string, string) error { return nil }

func (l *gwALimiter) Refresh(_ context.Context, tenantID, userID string, ttl time.Duration) error {
	l.refreshes = append(l.refreshes, gwARefresh{tenantID, userID, ttl})
	return nil
}

type gwARig struct {
	log     *gwALog
	store   *gwAStore
	sink    *gwASink
	limiter *gwALimiter
	svc     *StreamService
}

func newGwARig() *gwARig {
	log := &gwALog{}
	store := newGwAStore(log)
	limiter := &gwALimiter{}
	return &gwARig{
		log:     log,
		store:   store,
		sink:    &gwASink{log: log},
		limiter: limiter,
		svc:     NewStreamService(nil, store, limiter, 5, gwAConnTTL, gwAHeartbeat),
	}
}

func (r *gwARig) serve(ctx context.Context, ticket domain.Ticket, lastEventID string) error {
	return r.svc.Serve(ctx, ticket, lastEventID, r.sink)
}

func gwATicket() domain.Ticket {
	return domain.Ticket{TenantID: gwATenant, UserID: gwAUser}
}

func gwAAdminTicket() domain.Ticket {
	return domain.Ticket{TenantID: gwATenant, UserID: gwAUser, TenantRoles: []string{"tenant_admin"}}
}

func gwAFrameOf(id, eventType string) domain.Frame {
	return domain.Frame{ID: id, EventType: eventType, Data: []byte(`{"id":"` + id + `"}`)}
}

func gwACursor(pairs ...string) string {
	m := map[string]string{}
	for i := 0; i < len(pairs); i += 2 {
		m[pairs[i]] = pairs[i+1]
	}
	return encodeCursor(m)
}

// --- Ingest check order (O-CMP-08) -------------------------------------------

func gwARoutes(t *testing.T) *domain.RoutingTable {
	t.Helper()
	yes, no := true, false
	table, err := domain.NewRoutingTable(domain.RoutingFile{
		Version: 1,
		Services: map[string]domain.ServiceRoutes{
			"gwaflow":  {Source: gwAWorkflowSrc, Callers: []string{gwAWorkflowCall}, TenantAdmins: &yes, Events: []string{"Thing"}},
			"gwaother": {Source: gwAOtherSrc, Callers: []string{gwAOtherCall}, TenantAdmins: &no, Events: []string{"Thing"}},
		},
	})
	require.NoError(t, err)
	return table
}

type gwAEventStore struct{ calls []string }

func (s *gwAEventStore) AppendEvent(_ context.Context, tenantID, userID, eventType string, _ []byte) error {
	s.calls = append(s.calls, "user:"+tenantID+":"+userID+":"+eventType)
	return nil
}

func (s *gwAEventStore) AppendBroadcastEvent(_ context.Context, tenantID, eventType string, _ []byte) error {
	s.calls = append(s.calls, "broadcast:"+tenantID+":"+eventType)
	return nil
}

func (s *gwAEventStore) AppendDepartmentEvent(_ context.Context, tenantID, departmentID, eventType string, _ []byte) error {
	s.calls = append(s.calls, "department:"+tenantID+":"+departmentID+":"+eventType)
	return nil
}

func TestGwAFanoutService_Ingest_ABadPayloadDoesNotChangeTheCheckOrder(t *testing.T) {
	const badPayload = `{"recipients":"not a list"}`
	cases := []struct {
		name      string
		check     CallerCheck
		in        IngestInput
		wantErr   error
		wantName  string
		notErrors []error
	}{
		{
			name:      "unknown route with a bad payload and a foreign caller is unknown",
			check:     CallerCheckOn,
			in:        IngestInput{Source: "gwa-unlisted-svc", Caller: gwAOtherCall, EventType: "Thing"},
			wantErr:   ErrUnknownEventType,
			wantName:  "",
			notErrors: []error{ErrCallerNotAllowed, ErrInvalidPayload},
		},
		{
			name:      "known route, caller not admitted, bad payload is refused for the caller",
			check:     CallerCheckOn,
			in:        IngestInput{Source: gwAWorkflowSrc, Caller: gwAOtherCall, EventType: "Thing"},
			wantErr:   ErrCallerNotAllowed,
			wantName:  "gwaflow.Thing",
			notErrors: []error{ErrUnknownEventType, ErrInvalidPayload},
		},
		{
			name:      "known route, no caller identity, bad payload is refused for the caller",
			check:     CallerCheckOn,
			in:        IngestInput{Source: gwAWorkflowSrc, Caller: "", EventType: "Thing"},
			wantErr:   ErrCallerNotAllowed,
			wantName:  "gwaflow.Thing",
			notErrors: []error{ErrUnknownEventType, ErrInvalidPayload},
		},
		{
			name:      "admitted caller with a bad payload reaches the payload check",
			check:     CallerCheckOn,
			in:        IngestInput{Source: gwAWorkflowSrc, Caller: gwAWorkflowCall, EventType: "Thing"},
			wantErr:   ErrInvalidPayload,
			wantName:  "gwaflow.Thing",
			notErrors: []error{ErrUnknownEventType, ErrCallerNotAllowed},
		},
		{
			name:      "caller check off lets a foreign caller reach the payload check",
			check:     CallerCheckOff,
			in:        IngestInput{Source: gwAWorkflowSrc, Caller: gwAOtherCall, EventType: "Thing"},
			wantErr:   ErrInvalidPayload,
			wantName:  "gwaflow.Thing",
			notErrors: []error{ErrUnknownEventType, ErrCallerNotAllowed},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &gwAEventStore{}
			in := tc.in
			in.TenantID = gwATenant
			in.Payload = []byte(badPayload)
			in.RawEnvelope = []byte(`{"raw":true}`)

			name, err := NewFanoutService(store, gwARoutes(t), tc.check).Ingest(context.Background(), in)

			require.ErrorIs(t, err, tc.wantErr)
			for _, other := range tc.notErrors {
				assert.NotErrorIs(t, err, other)
			}
			assert.Equal(t, tc.wantName, name)
			assert.Empty(t, store.calls, "nothing is written for any of these refusals")
		})
	}
}

// --- streamTargets (O-CMP-10, O-CMP-11) --------------------------------------

func TestGwAStreamTargets_TenantWideRolesGetTheUserAndTheTenantTargetOnly(t *testing.T) {
	cases := []struct {
		name  string
		roles []string
	}{
		{name: "tenant_owner", roles: []string{"tenant_owner"}},
		{name: "tenant_admin", roles: []string{"tenant_admin"}},
		{name: "owner listed after an ordinary role", roles: []string{"member", "tenant_owner"}},
		{name: "both admin and owner give one tenant target", roles: []string{"tenant_admin", "tenant_owner"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ticket := domain.Ticket{
				TenantID:    gwATenant,
				UserID:      gwAUser,
				TenantRoles: tc.roles,
				Departments: []domain.DepartmentRole{{DepartmentID: gwADeptOne, Role: "approver"}, {DepartmentID: gwADeptTwo, Role: "reviewer"}},
			}

			got := streamTargets(ticket)

			assert.Equal(t, []StreamTarget{gwAPersonal, gwABroadcast}, got, "user target first, tenant-wide target second, no department targets")
		})
	}
}

func TestGwAStreamTargets_DepartmentsOfAnOrdinaryUser(t *testing.T) {
	cases := []struct {
		name        string
		roles       []string
		departments []domain.DepartmentRole
		want        []StreamTarget
	}{
		{
			name:        "an empty department id is skipped, repeats collapse, order is kept",
			departments: []domain.DepartmentRole{{DepartmentID: gwADeptOne}, {DepartmentID: gwADeptOne}, {DepartmentID: ""}, {DepartmentID: gwADeptTwo}},
			want:        []StreamTarget{gwAPersonal, gwADeptA, gwADeptB},
		},
		{
			name:        "only empty department ids leave the user target alone",
			departments: []domain.DepartmentRole{{DepartmentID: ""}, {DepartmentID: ""}},
			want:        []StreamTarget{gwAPersonal},
		},
		{
			name: "no departments leave the user target alone",
			want: []StreamTarget{gwAPersonal},
		},
		{
			name:        "a role that is not admin or owner keeps the departments",
			roles:       []string{"member", "admin"},
			departments: []domain.DepartmentRole{{DepartmentID: gwADeptTwo}},
			want:        []StreamTarget{gwAPersonal, gwADeptB},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := streamTargets(domain.Ticket{TenantID: gwATenant, UserID: gwAUser, TenantRoles: tc.roles, Departments: tc.departments})

			assert.Equal(t, tc.want, got)
		})
	}
}

// --- cursor keys outside the ticket (O-CMP-12, P-2-MT-01) --------------------

func TestGwAStreamService_Serve_CursorKeysOutsideTheTicketTargetsAreIgnored(t *testing.T) {
	cases := []struct {
		name        string
		ticket      domain.Ticket
		lastEventID string
		wantTargets []StreamTarget
	}{
		{
			name:        "a cursor naming only a key the ticket does not have asks for no backfill",
			ticket:      gwATicket(),
			lastEventID: gwACursor(gwAKey(gwADeptA), "10-0"),
			wantTargets: []StreamTarget{gwAPersonal},
		},
		{
			name:        "a downgraded role leaves the tenant-wide cursor entry unused",
			ticket:      gwATicket(),
			lastEventID: gwACursor(gwAKey(gwAPersonal), "10-0", gwAKey(gwABroadcast), "10-0"),
			wantTargets: []StreamTarget{gwAPersonal},
		},
		{
			name:        "no cursor at all asks for no backfill",
			ticket:      gwATicket(),
			lastEventID: "",
			wantTargets: []StreamTarget{gwAPersonal},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newGwARig()
			for _, target := range []StreamTarget{gwAPersonal, gwABroadcast, gwADeptA} {
				key := gwAKey(target)
				rig.store.latest[key] = "900-0"
				// Every key holds a replayable window, so a key that is asked for shows in the log.
				rig.store.oldest[key] = gwAOldest{id: "5-0", exists: true}
				rig.store.ranges[key] = gwARange{frames: []domain.Frame{gwAFrameOf("20-0", "gwa.Replayed")}}
			}

			err := rig.serve(context.Background(), tc.ticket, tc.lastEventID)

			require.ErrorIs(t, err, errGwAStop)
			assert.False(t, rig.log.has("oldest:"+gwAKey(gwABroadcast)), "the foreign key's oldest id is never read")
			assert.False(t, rig.log.has("range:"+gwAKey(gwABroadcast)), "the foreign key is never replayed")
			assert.False(t, rig.log.has("oldest:"+gwAKey(gwADeptA)))
			assert.False(t, rig.log.has("range:"+gwAKey(gwADeptA)))
			assert.Empty(t, rig.sink.control, "no resync is signalled for a key outside the ticket")
			require.NotEmpty(t, rig.store.reads)
			var got []StreamTarget
			for _, p := range rig.store.reads[0] {
				got = append(got, p.Target)
			}
			assert.Equal(t, tc.wantTargets, got, "only the ticket's own targets are read")
		})
	}
}

func TestGwAStreamService_Serve_ACursorNamingAnotherUsersOrTenantsStreamIsNotReplayed(t *testing.T) {
	foreign := []StreamTarget{
		{TenantID: gwATenant, UserID: gwAOtherUser},
		{TenantID: gwAOtherTenant, UserID: gwAUser},
		{TenantID: gwAOtherTenant, UserID: gwAOtherUser},
		{TenantID: gwAOtherTenant},
		{TenantID: gwAOtherTenant, DepartmentID: gwADeptOne},
	}
	rig := newGwARig()
	pairs := []string{}
	for _, f := range foreign {
		key := gwAKey(f)
		rig.store.latest[key] = "900-0"
		rig.store.oldest[key] = gwAOldest{id: "5-0", exists: true}
		rig.store.ranges[key] = gwARange{frames: []domain.Frame{gwAFrameOf("20-0", "gwa.Foreign")}}
		pairs = append(pairs, key, "0-0")
	}
	own := gwAKey(gwAPersonal)
	rig.store.latest[own] = "900-0"
	rig.store.oldest[own] = gwAOldest{id: "5-0", exists: true}
	rig.store.ranges[own] = gwARange{frames: []domain.Frame{gwAFrameOf("30-0", "gwa.Own")}}
	pairs = append(pairs, own, "10-0")
	rig.store.readSteps = []gwAReadStep{{frames: []domain.Frame{{ID: "950-0", EventType: "gwa.Live", Data: []byte(`{}`), SourceKey: own}}}}

	err := rig.serve(context.Background(), gwATicket(), gwACursor(pairs...))

	require.ErrorIs(t, err, errGwAStop)
	for _, f := range foreign {
		assert.False(t, rig.log.has("oldest:"+gwAKey(f)), "oldest id of %s is not read", gwAKey(f))
		assert.False(t, rig.log.has("range:"+gwAKey(f)), "%s is not replayed", gwAKey(f))
	}
	require.Len(t, rig.sink.frames, 2, "the own replay frame and the own live frame only")
	assert.Equal(t, "gwa.Own", rig.sink.frames[0].eventType)
	assert.Equal(t, "gwa.Live", rig.sink.frames[1].eventType)
	assert.Empty(t, rig.sink.control)
	for _, f := range rig.sink.frames {
		for _, other := range foreign {
			assert.NotContains(t, decodeCursor(f.id), gwAKey(other), "the resume cursor never names a foreign stream")
		}
	}
	for _, read := range rig.store.reads {
		require.Len(t, read, 1)
		assert.Equal(t, gwAPersonal, read[0].Target)
	}
}

// --- backfill window edges (O-CMP-13, O-CMP-14, P-2-EC-05) ------------------

const gwAResyncBody = `{"reason":"backfill_window_exceeded"}`

func TestGwAStreamService_Serve_TheBackfillWindowBoundary(t *testing.T) {
	cases := []struct {
		name       string
		cursorID   string
		oldest     gwAOldest
		wantReplay bool
	}{
		{name: "oldest equal to the cursor replays", cursorID: "140-0", oldest: gwAOldest{id: "140-0", exists: true}, wantReplay: true},
		{name: "oldest before the cursor replays", cursorID: "140-0", oldest: gwAOldest{id: "139-9", exists: true}, wantReplay: true},
		{name: "oldest one sequence step after the cursor resyncs", cursorID: "140-0", oldest: gwAOldest{id: "140-1", exists: true}},
		{name: "oldest one millisecond after the cursor resyncs", cursorID: "140-5", oldest: gwAOldest{id: "141-0", exists: true}},
		{name: "empty stream resyncs", cursorID: "140-0", oldest: gwAOldest{exists: false}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newGwARig()
			key := gwAKey(gwAPersonal)
			rig.store.latest[key] = "500-0"
			rig.store.oldest[key] = tc.oldest
			rig.store.ranges[key] = gwARange{frames: []domain.Frame{gwAFrameOf("150-0", "gwa.Replayed")}}

			err := rig.serve(context.Background(), gwATicket(), gwACursor(key, tc.cursorID))

			require.ErrorIs(t, err, errGwAStop)
			if tc.wantReplay {
				assert.Equal(t, tc.cursorID, rig.store.rangeAfter[key], "the replay starts after the client's cursor")
				require.Len(t, rig.sink.frames, 1)
				assert.Equal(t, "gwa.Replayed", rig.sink.frames[0].eventType)
				assert.Empty(t, rig.sink.control, "no resync when the window still holds the cursor")
				assert.Equal(t, "150-0", rig.store.reads[0][0].AfterID, "live reads continue after the last replayed frame")
				return
			}
			assert.False(t, rig.log.has("range:"+key), "a stale cursor is never replayed")
			assert.Empty(t, rig.sink.frames)
			require.Len(t, rig.sink.control, 1)
			assert.Equal(t, "resync-required", rig.sink.control[0].eventType)
			assert.JSONEq(t, gwAResyncBody, string(rig.sink.control[0].data))
			assert.Equal(t, "500-0", rig.store.reads[0][0].AfterID, "after a resync live reads start from the newest entry at connect")
		})
	}
}

func TestGwAStreamService_Serve_OneTargetBeyondTheWindowDropsTheOtherTargetsReplay(t *testing.T) {
	rig := newGwARig()
	personal, broadcast := gwAKey(gwAPersonal), gwAKey(gwABroadcast)
	rig.store.latest[personal] = "400-0"
	rig.store.latest[broadcast] = "410-0"
	rig.store.oldest[personal] = gwAOldest{id: "100-0", exists: true}
	rig.store.ranges[personal] = gwARange{frames: []domain.Frame{gwAFrameOf("150-0", "gwa.A"), gwAFrameOf("160-0", "gwa.B")}}
	rig.store.oldest[broadcast] = gwAOldest{id: "500-0", exists: true}
	rig.store.ranges[broadcast] = gwARange{frames: []domain.Frame{gwAFrameOf("510-0", "gwa.C")}}

	err := rig.serve(context.Background(), gwAAdminTicket(), gwACursor(personal, "140-0", broadcast, "140-0"))

	require.ErrorIs(t, err, errGwAStop)
	assert.True(t, rig.log.has("range:"+personal), "the first target's replay was fetched before the second target was checked")
	assert.False(t, rig.log.has("range:"+broadcast))
	assert.Empty(t, rig.sink.frames, "the first target's fetched frames are dropped")
	require.Len(t, rig.sink.control, 1, "exactly one resync for the whole connection")
	assert.JSONEq(t, gwAResyncBody, string(rig.sink.control[0].data))
	assert.Less(t, rig.log.indexOf("range:"+personal), rig.log.indexOf("oldest:"+broadcast), "targets are checked in order")
	require.NotEmpty(t, rig.store.reads)
	assert.Equal(t, []StreamPosition{
		{Target: gwAPersonal, AfterID: "400-0"},
		{Target: gwABroadcast, AfterID: "410-0"},
	}, rig.store.reads[0], "live reads ignore the stale cursors and start at the newest entries")
}

func TestGwAStreamService_Serve_ResyncVersusReplayOrdersIdsNumerically(t *testing.T) {
	cases := []struct {
		name       string
		cursorID   string
		oldestID   string
		wantResync bool
	}{
		{name: "equal ids replay", cursorID: "1700-3", oldestID: "1700-3"},
		{name: "oldest 9 against cursor 10 replays (numeric, not text, order)", cursorID: "10-0", oldestID: "9-0"},
		{name: "oldest 10 against cursor 9 resyncs (numeric, not text, order)", cursorID: "9-0", oldestID: "10-0", wantResync: true},
		{name: "same millisecond, oldest sequence 10 against cursor 9 resyncs", cursorID: "50-9", oldestID: "50-10", wantResync: true},
		{name: "same millisecond, oldest sequence 9 against cursor 10 replays", cursorID: "50-10", oldestID: "50-9"},
		{name: "oldest millisecond later resyncs even with a lower sequence", cursorID: "50-9", oldestID: "51-0", wantResync: true},
		{name: "oldest millisecond earlier replays even with a higher sequence", cursorID: "51-0", oldestID: "50-9"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newGwARig()
			key := gwAKey(gwAPersonal)
			rig.store.oldest[key] = gwAOldest{id: tc.oldestID, exists: true}
			rig.store.ranges[key] = gwARange{frames: []domain.Frame{gwAFrameOf("99999-0", "gwa.Replayed")}}

			err := rig.serve(context.Background(), gwATicket(), gwACursor(key, tc.cursorID))

			require.ErrorIs(t, err, errGwAStop)
			if tc.wantResync {
				assert.Len(t, rig.sink.control, 1)
				assert.Empty(t, rig.sink.frames)
				assert.False(t, rig.log.has("range:"+key))
				return
			}
			assert.Empty(t, rig.sink.control)
			require.Len(t, rig.sink.frames, 1)
			assert.Equal(t, tc.cursorID, rig.store.rangeAfter[key])
		})
	}
}

func TestGwACompareStreamIDs_NumericAndMalformedIds(t *testing.T) {
	cases := []struct {
		name string
		a, b string
		want int
	}{
		{name: "9 against 10 is numeric", a: "9-0", b: "10-0", want: -1},
		{name: "10 against 9 is numeric", a: "10-0", b: "9-0", want: 1},
		{name: "sequence 9 against 10 is numeric", a: "5-9", b: "5-10", want: -1},
		{name: "sequence 10 against 9 is numeric", a: "5-10", b: "5-9", want: 1},
		{name: "id without a dash has sequence 0 and equals the explicit form", a: "100", b: "100-0", want: 0},
		{name: "id without a dash is below a higher sequence", a: "100", b: "100-1", want: -1},
		{name: "id without a dash is above a lower millisecond", a: "100", b: "99-5", want: 1},
		{name: "two ids without a dash compare by millisecond", a: "200", b: "100", want: 1},
		{name: "non-numeric id is zero zero", a: "not-an-id", b: "0-0", want: 0},
		{name: "empty id is zero zero", a: "", b: "0-0", want: 0},
		{name: "empty id is below a real one", a: "", b: "1-0", want: -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, compareStreamIDs(tc.a, tc.b))
			assert.Equal(t, -tc.want, compareStreamIDs(tc.b, tc.a), "the comparison is antisymmetric")
		})
	}
}

// --- backfill frames and errors (O-CMP-16, O-CMP-22) -------------------------

func TestGwAStreamService_Serve_BackfillFramesCarryTheLatestIdOfEveryKey(t *testing.T) {
	rig := newGwARig()
	personal, broadcast := gwAKey(gwAPersonal), gwAKey(gwABroadcast)
	rig.store.latest[personal] = "400-0"
	rig.store.latest[broadcast] = "410-0"
	rig.store.oldest[personal] = gwAOldest{id: "100-0", exists: true}
	rig.store.oldest[broadcast] = gwAOldest{id: "100-0", exists: true}
	rig.store.ranges[personal] = gwARange{frames: []domain.Frame{gwAFrameOf("150-0", "gwa.A"), gwAFrameOf("160-0", "gwa.B")}}
	rig.store.ranges[broadcast] = gwARange{frames: []domain.Frame{gwAFrameOf("155-0", "gwa.C")}}

	err := rig.serve(context.Background(), gwAAdminTicket(), gwACursor(personal, "140-0", broadcast, "145-0"))

	require.ErrorIs(t, err, errGwAStop)
	require.Len(t, rig.sink.frames, 3)
	want := []map[string]string{
		{personal: "150-0", broadcast: "145-0"},
		{personal: "160-0", broadcast: "145-0"},
		{personal: "160-0", broadcast: "155-0"},
	}
	for i, f := range rig.sink.frames {
		assert.Equal(t, want[i], decodeCursor(f.id), "frame %d carries the latest id seen for each key", i)
	}
	assert.Equal(t, []string{"gwa.A", "gwa.B", "gwa.C"}, []string{rig.sink.frames[0].eventType, rig.sink.frames[1].eventType, rig.sink.frames[2].eventType})
	assert.Equal(t, []StreamPosition{
		{Target: gwAPersonal, AfterID: "160-0"},
		{Target: gwABroadcast, AfterID: "155-0"},
	}, rig.store.reads[0])
}

func TestGwAStreamService_Serve_ABackfillFrameWriteFailureStopsServe(t *testing.T) {
	cases := []struct {
		name       string
		failOn     int
		wantFrames int
	}{
		{name: "first backfill frame fails", failOn: 1, wantFrames: 0},
		{name: "second backfill frame fails", failOn: 2, wantFrames: 1},
		{name: "last backfill frame fails", failOn: 3, wantFrames: 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newGwARig()
			personal, broadcast := gwAKey(gwAPersonal), gwAKey(gwABroadcast)
			rig.store.oldest[personal] = gwAOldest{id: "100-0", exists: true}
			rig.store.oldest[broadcast] = gwAOldest{id: "100-0", exists: true}
			rig.store.ranges[personal] = gwARange{frames: []domain.Frame{gwAFrameOf("150-0", "gwa.A"), gwAFrameOf("160-0", "gwa.B")}}
			rig.store.ranges[broadcast] = gwARange{frames: []domain.Frame{gwAFrameOf("155-0", "gwa.C")}}
			rig.sink.frameErrOn = tc.failOn

			err := rig.serve(context.Background(), gwAAdminTicket(), gwACursor(personal, "140-0", broadcast, "140-0"))

			require.ErrorIs(t, err, errGwABoom)
			assert.NotErrorIs(t, err, errGwAStop)
			assert.Len(t, rig.sink.frames, tc.wantFrames, "no frame is written after the failing one")
			assert.Equal(t, tc.failOn, rig.sink.frameCalls)
			assert.Empty(t, rig.sink.control)
			assert.Zero(t, rig.log.count("read"), "the live loop never starts")
		})
	}
}

func TestGwAStreamService_Serve_AReplayStoreFailureWritesNothing(t *testing.T) {
	cases := []struct {
		name  string
		setup func(personal, broadcast string, s *gwAStore)
	}{
		{
			name: "oldest id fails on the first target",
			setup: func(personal, _ string, s *gwAStore) {
				s.oldest[personal] = gwAOldest{err: errGwABoom}
			},
		},
		{
			name: "oldest id fails on the second target after the first replayed",
			setup: func(personal, broadcast string, s *gwAStore) {
				s.oldest[personal] = gwAOldest{id: "100-0", exists: true}
				s.ranges[personal] = gwARange{frames: []domain.Frame{gwAFrameOf("150-0", "gwa.A")}}
				s.oldest[broadcast] = gwAOldest{err: errGwABoom}
			},
		},
		{
			name: "range fails on the first target",
			setup: func(personal, _ string, s *gwAStore) {
				s.oldest[personal] = gwAOldest{id: "100-0", exists: true}
				s.ranges[personal] = gwARange{err: errGwABoom}
			},
		},
		{
			name: "range fails on the second target after the first replayed",
			setup: func(personal, broadcast string, s *gwAStore) {
				s.oldest[personal] = gwAOldest{id: "100-0", exists: true}
				s.ranges[personal] = gwARange{frames: []domain.Frame{gwAFrameOf("150-0", "gwa.A"), gwAFrameOf("160-0", "gwa.B")}}
				s.oldest[broadcast] = gwAOldest{id: "100-0", exists: true}
				s.ranges[broadcast] = gwARange{err: errGwABoom}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newGwARig()
			personal, broadcast := gwAKey(gwAPersonal), gwAKey(gwABroadcast)
			tc.setup(personal, broadcast, rig.store)

			err := rig.serve(context.Background(), gwAAdminTicket(), gwACursor(personal, "140-0", broadcast, "140-0"))

			require.ErrorIs(t, err, errGwABoom)
			assert.Empty(t, rig.sink.frames, "frames fetched for an earlier target are discarded")
			assert.Empty(t, rig.sink.control, "no resync is written for a store failure")
			assert.Zero(t, rig.sink.heartbeats)
			assert.Zero(t, rig.log.count("read"))
			assert.Zero(t, rig.log.count("frame")+rig.log.count("control")+rig.log.count("heartbeat"), "the sink is never called")
		})
	}
}

// --- the newest-entry lookup at connect (O-CMP-46, P-2-DF-02) ---------------

func TestGwAStreamService_Serve_ANewestEntryFailureEndsServeBeforeAnythingElse(t *testing.T) {
	cases := []struct {
		name       string
		failKey    string
		wantLatest []string
	}{
		{name: "first target fails", failKey: gwAKey(gwAPersonal), wantLatest: []string{"latest:" + gwAKey(gwAPersonal)}},
		{name: "second target fails", failKey: gwAKey(gwABroadcast), wantLatest: []string{"latest:" + gwAKey(gwAPersonal), "latest:" + gwAKey(gwABroadcast)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newGwARig()
			personal, broadcast := gwAKey(gwAPersonal), gwAKey(gwABroadcast)
			rig.store.latest[personal] = "400-0"
			rig.store.latestErr[tc.failKey] = errGwABoom
			rig.store.oldest[personal] = gwAOldest{id: "100-0", exists: true}
			rig.store.oldest[broadcast] = gwAOldest{id: "100-0", exists: true}

			err := rig.serve(context.Background(), gwAAdminTicket(), gwACursor(personal, "140-0", broadcast, "140-0"))

			require.ErrorIs(t, err, errGwABoom)
			assert.Equal(t, tc.wantLatest, rig.log.entries, "only the lookups up to the failing one ran: no oldest id, no range, no read, no sink write")
			assert.Empty(t, rig.sink.frames)
			assert.Empty(t, rig.sink.control)
			assert.Zero(t, rig.sink.heartbeats)
		})
	}
}

func TestGwAStreamService_Serve_TheNewestEntryOfEveryTargetIsReadBeforeAnyFrame(t *testing.T) {
	rig := newGwARig()
	personal, dept := gwAKey(gwAPersonal), gwAKey(gwADeptA)
	ticket := domain.Ticket{TenantID: gwATenant, UserID: gwAUser, Departments: []domain.DepartmentRole{{DepartmentID: gwADeptOne}}}
	rig.store.oldest[personal] = gwAOldest{id: "100-0", exists: true}
	rig.store.ranges[personal] = gwARange{frames: []domain.Frame{gwAFrameOf("150-0", "gwa.A")}}
	rig.store.oldest[dept] = gwAOldest{id: "100-0", exists: true}
	rig.store.ranges[dept] = gwARange{frames: []domain.Frame{gwAFrameOf("155-0", "gwa.B")}}

	err := rig.serve(context.Background(), ticket, gwACursor(personal, "140-0", dept, "140-0"))

	require.ErrorIs(t, err, errGwAStop)
	require.Len(t, rig.sink.frames, 2)
	firstFrame := rig.log.indexOf("frame")
	require.GreaterOrEqual(t, firstFrame, 2)
	assert.Equal(t, []string{"latest:" + personal, "latest:" + dept}, rig.log.entries[:2], "every target's newest entry is read first, in target order")
	assert.Equal(t, 2, rig.log.count("latest:"))
}

// --- the live loop (O-CMP-17, P-2-DF-01, P-2-EC-04) -------------------------

func TestGwAStreamService_Serve_TheLiveLoopOutcomes(t *testing.T) {
	personal := gwAKey(gwAPersonal)
	live := func(id string) domain.Frame {
		return domain.Frame{ID: id, EventType: "gwa.Live", Data: []byte(`{}`), SourceKey: personal}
	}
	cases := []struct {
		name           string
		steps          func(cancel context.CancelFunc) []gwAReadStep
		sink           func(s *gwASink)
		wantErr        error // nil means Serve returns nil
		wantStopErr    bool
		wantHeartbeats int
		wantFrames     int
		wantRefreshes  int
		wantReads      int
	}{
		{
			name:           "an empty read writes a heartbeat and keeps reading",
			steps:          func(context.CancelFunc) []gwAReadStep { return []gwAReadStep{{}, {}} },
			wantStopErr:    true,
			wantHeartbeats: 2,
			wantRefreshes:  2,
			wantReads:      3,
		},
		{
			name:          "a read error with a live context is returned",
			steps:         func(context.CancelFunc) []gwAReadStep { return []gwAReadStep{{err: errGwABoom}} },
			wantErr:       errGwABoom,
			wantReads:     1,
			wantRefreshes: 0,
		},
		{
			name: "the same read error after the client left ends Serve normally",
			steps: func(cancel context.CancelFunc) []gwAReadStep {
				return []gwAReadStep{{err: errGwABoom, run: cancel}}
			},
			wantReads: 1,
		},
		{
			name: "a frame delivered while the client leaves ends Serve at the top of the loop",
			steps: func(cancel context.CancelFunc) []gwAReadStep {
				return []gwAReadStep{{frames: []domain.Frame{live("700-0")}, run: cancel}}
			},
			wantFrames:    1,
			wantReads:     1,
			wantRefreshes: 1,
		},
		{
			name:  "a heartbeat write failure is returned",
			steps: func(context.CancelFunc) []gwAReadStep { return []gwAReadStep{{}, {}} },
			sink: func(s *gwASink) {
				s.heartbeatErr = errGwABoom
			},
			wantErr:       errGwABoom,
			wantReads:     1,
			wantRefreshes: 1,
		},
		{
			name: "a live frame write failure is returned",
			steps: func(context.CancelFunc) []gwAReadStep {
				return []gwAReadStep{{frames: []domain.Frame{live("700-0")}}, {}}
			},
			sink:          func(s *gwASink) { s.frameErrOn = 1 },
			wantErr:       errGwABoom,
			wantReads:     1,
			wantRefreshes: 1,
		},
		{
			name: "a failure on the second frame of a batch is returned and the rest is dropped",
			steps: func(context.CancelFunc) []gwAReadStep {
				return []gwAReadStep{{frames: []domain.Frame{live("700-0"), live("701-0"), live("702-0")}}}
			},
			sink:          func(s *gwASink) { s.frameErrOn = 2 },
			wantErr:       errGwABoom,
			wantFrames:    1,
			wantReads:     1,
			wantRefreshes: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newGwARig()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			rig.store.latest[personal] = "600-0"
			rig.store.readSteps = tc.steps(cancel)
			if tc.sink != nil {
				tc.sink(rig.sink)
			}

			err := rig.serve(ctx, gwATicket(), "")

			switch {
			case tc.wantErr != nil:
				require.ErrorIs(t, err, tc.wantErr)
				assert.NotErrorIs(t, err, errGwAStop)
			case tc.wantStopErr:
				require.ErrorIs(t, err, errGwAStop)
			default:
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantHeartbeats, rig.sink.heartbeats)
			assert.Len(t, rig.sink.frames, tc.wantFrames)
			assert.Len(t, rig.limiter.refreshes, tc.wantRefreshes)
			for _, r := range rig.limiter.refreshes {
				assert.Equal(t, gwARefresh{tenantID: gwATenant, userID: gwAUser, ttl: gwAConnTTL}, r)
			}
			assert.Len(t, rig.store.reads, tc.wantReads)
			for _, b := range rig.store.blocks {
				assert.Equal(t, gwAHeartbeat, b, "every live read blocks for the heartbeat interval")
			}
		})
	}
}

func TestGwAStreamService_Serve_AControlFrameWriteFailureIsReturnedBeforeTheLiveLoop(t *testing.T) {
	rig := newGwARig()
	key := gwAKey(gwAPersonal)
	rig.store.oldest[key] = gwAOldest{exists: false}
	rig.sink.controlErr = errGwABoom

	err := rig.serve(context.Background(), gwATicket(), gwACursor(key, "10-0"))

	require.ErrorIs(t, err, errGwABoom)
	assert.Zero(t, rig.log.count("read"))
	assert.Zero(t, rig.log.count("frame"))
}

func TestGwAStreamService_Serve_ACancelledContextNeverReadsOrWrites(t *testing.T) {
	rig := newGwARig()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := rig.serve(ctx, gwATicket(), "")

	require.NoError(t, err)
	assert.Zero(t, rig.log.count("read"))
	assert.Empty(t, rig.limiter.refreshes)
	assert.Zero(t, rig.sink.heartbeats)
}

// --- ticket token shape (O-SEC-03) ------------------------------------------

type gwARecordingReader struct {
	fill  byte
	asked []int
}

func (r *gwARecordingReader) Read(p []byte) (int, error) {
	r.asked = append(r.asked, len(p))
	for i := range p {
		p[i] = r.fill
	}
	return len(p), nil
}

// gwAShortReader hands out 5 bytes and then reports the end of its input.
type gwAShortReader struct{ gaveBytes bool }

func (r *gwAShortReader) Read(p []byte) (int, error) {
	if r.gaveBytes {
		return 0, io.EOF
	}
	r.gaveBytes = true
	return copy(p, []byte{1, 2, 3, 4, 5}), nil
}

func TestGwAGenerateToken_IsTheRawURLEncodingOf32RandomBytes(t *testing.T) {
	original := randReader
	defer func() { randReader = original }()

	t.Run("reads exactly 32 bytes and encodes them without padding", func(t *testing.T) {
		reader := &gwARecordingReader{fill: 0xfb}
		randReader = reader

		token, err := generateToken()

		require.NoError(t, err)
		assert.Equal(t, []int{32}, reader.asked, "one read of 32 bytes")
		want := make([]byte, 32)
		for i := range want {
			want[i] = 0xfb
		}
		assert.Equal(t, base64.RawURLEncoding.EncodeToString(want), token)
		assert.Len(t, token, 43)
		assert.Contains(t, token, "-", "0xfb bytes encode to the URL alphabet's dash, not the standard plus")
		assert.NotContains(t, token, "+")
		assert.NotContains(t, token, "/")
		assert.NotContains(t, token, "=")
	})

	t.Run("a short read is an error and no token is returned", func(t *testing.T) {
		randReader = &gwAShortReader{}

		token, err := generateToken()

		require.Error(t, err)
		assert.Empty(t, token)
	})

	t.Run("the system source gives 43 characters from the URL-safe alphabet", func(t *testing.T) {
		randReader = original
		alphabet := regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)

		seen := map[string]bool{}
		for range 20 {
			token, err := generateToken()
			require.NoError(t, err)
			assert.Regexp(t, alphabet, token)
			seen[token] = true
		}
		assert.Len(t, seen, 20, "twenty tokens are all different")
	})
}
