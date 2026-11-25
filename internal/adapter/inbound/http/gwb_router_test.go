package http

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGwB_Healthz_AnswersOkJSONWithoutAnyIdentity(t *testing.T) {
	w := newGwBWiring()
	r := w.router()

	rec := w.serve(r, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, strings.HasPrefix(rec.Header().Get("Content-Type"), "application/json"), rec.Header().Get("Content-Type"))
	assert.JSONEq(t, `{"status":"ok"}`, rec.Body.String())
	assert.Empty(t, w.tickets.gets, "the probe touches no store")
}

// An encoded slash is refused with the gateway's not-found problem, whichever route it targets.
// None of these calls carries an identity, so a 404 (not 401) shows the refusal runs before any
// group's identity check, and the correlation id in the body shows it runs inside the
// correlation middleware.
func TestGwB_EncodedSlash_IsANotFoundProblemBeforeEveryIdentityCheck(t *testing.T) {
	cases := []struct{ method, target string }{
		{http.MethodPost, "/api/v1/internal%2Fevents"},
		{http.MethodPost, "/api/v1/internal%2fevents"},
		{http.MethodPost, "/api/v1%2Finternal/events"},
		{http.MethodPost, "/api/v1/stream%2Ftickets"},
		{http.MethodGet, "/api/v1/stream%2f"},
		{http.MethodGet, "/healthz%2F"},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.target, func(t *testing.T) {
			w := newGwBWiring()
			r := w.router()

			rec := w.serve(r, httptest.NewRequest(tc.method, tc.target, nil))

			gwBAssertProblemShape(t, rec, http.StatusNotFound, "not-found")
			assert.Zero(t, w.events.total())
			assert.Empty(t, w.tickets.gets)
			assert.Empty(t, w.tickets.puts)
			assert.Empty(t, w.limiter.acquires)
		})
	}
}

func TestGwB_Router_OnlyTheTicketAndInternalGroupsCarryTheRequestDeadline(t *testing.T) {
	w := newGwBWiring()
	r := w.router()
	w.tickets.tickets["tok"] = gwBTicketFor(testTenantID, testUserID)
	start := time.Now()

	require.Equal(t, http.StatusCreated, w.serve(r, gwBMintRequest(t)).Code)
	require.Equal(t, http.StatusOK, w.serve(r, gwBSystemPost(gwBEventsPath, gwBValidEnvelope)).Code)
	require.Equal(t, http.StatusOK, w.serve(r, httptest.NewRequest(http.MethodGet, "/api/v1/stream?ticket=tok", nil)).Code)

	require.True(t, w.tickets.allowHasDeadline, "ticket mint runs under the short request timeout")
	assert.False(t, w.tickets.allowDeadline.Before(start.Add(shortRequestTimeout)))
	assert.False(t, w.tickets.allowDeadline.After(time.Now().Add(shortRequestTimeout)))

	require.True(t, w.events.hasDeadline, "event ingest runs under the short request timeout")
	assert.False(t, w.events.deadline.Before(start.Add(shortRequestTimeout)))
	assert.False(t, w.events.deadline.After(time.Now().Add(shortRequestTimeout)))

	assert.False(t, w.tickets.getHasDeadline, "an open stream must outlive the short request timeout")
}

// APP_ENV dev and local keep gin's mode; any other value switches it to release.
func TestGwB_NewRouter_GinModeFollowsTheEnvironment(t *testing.T) {
	t.Cleanup(func() { gin.SetMode(gin.TestMode) })
	cases := []struct {
		appEnv   string
		wantMode string
	}{
		{"dev", gin.DebugMode},
		{"local", gin.DebugMode},
		{"test", gin.ReleaseMode},
		{"staging", gin.ReleaseMode},
		{"production", gin.ReleaseMode},
		{"", gin.ReleaseMode},
	}
	for _, tc := range cases {
		t.Run("APP_ENV="+tc.appEnv, func(t *testing.T) {
			cfg := newGwBWiring().routerConfig()
			cfg.AppEnv = tc.appEnv
			gin.SetMode(gin.DebugMode)

			NewRouter(cfg)

			assert.Equal(t, tc.wantMode, gin.Mode())
		})
	}
}

// The groups answer to different identity rules: gateway identity for the mint, the system
// caller for the internal route; neither accepts the other's headers.
func TestGwB_Router_EachGroupAnswersWithItsOwnIdentityRule(t *testing.T) {
	cases := []struct {
		name       string
		build      func() *http.Request
		wantStatus int
	}{
		{"mint with gateway identity", func() *http.Request { return gwBMintRequest(t) }, http.StatusCreated},
		{"mint with only the system headers", func() *http.Request {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/stream/tickets", nil)
			req.Header.Set("x-user-id", "iam-system")
			req.Header.Set("x-tenant-roles", "iam-system")
			return req
		}, http.StatusUnauthorized},
		{"internal with gateway identity but no system role", func() *http.Request {
			req := gwBMintRequest(t)
			req.URL.Path = gwBEventsPath
			req.Header.Set("x-tenant-roles", "tenant_admin")
			return req
		}, http.StatusUnauthorized},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newGwBWiring()
			r := w.router()

			rec := w.serve(r, tc.build())

			assert.Equal(t, tc.wantStatus, rec.Code, rec.Body.String())
		})
	}
}
