package http

import (
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

// Called with no request context at all (the middleware never ran), Mint answers with the
// gateway's own unauthorized problem and mints nothing.
func TestGwB_TicketMint_WithoutARequestContextIsAnUnauthorizedProblem(t *testing.T) {
	obstest.Init(t)
	gin.SetMode(gin.TestMode)
	w := newGwBWiring()
	handler := NewTicketHandler(usecase.NewTicketService(w.tickets, 60*time.Second, 10, time.Minute))
	beforeAll := obstest.Sum(t, gwBMintedMetric, nil)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/stream/tickets", nil)

	handler.Mint(c)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))
	assert.JSONEq(t, `{
		"type": "`+gwBProblemBase+`unauthorized",
		"title": "Unauthorized",
		"status": 401,
		"detail": "missing gateway identity headers",
		"instance": "/api/v1/stream/tickets",
		"code": "unauthorized"
	}`, rec.Body.String())
	assert.Empty(t, w.tickets.allows, "no rate check")
	assert.Empty(t, w.tickets.puts, "no ticket stored")
	assert.Equal(t, beforeAll, obstest.Sum(t, gwBMintedMetric, nil), "a refused mint is not counted")
}

// Through the real router a call missing the tenant or the user never reaches the store: the
// platform's identity middleware refuses it with a 401 first.
func TestGwB_TicketMint_ACallMissingTheTenantOrTheUserIsRefusedWithNothingMinted(t *testing.T) {
	cases := map[string]func(req *http.Request){
		"no tenant":      func(req *http.Request) { req.Header.Del("x-tenant-id") },
		"no user":        func(req *http.Request) { req.Header.Del("x-user-id") },
		"blank tenant":   func(req *http.Request) { req.Header.Set("x-tenant-id", "  ") },
		"blank user":     func(req *http.Request) { req.Header.Set("x-user-id", "") },
		"neither header": func(req *http.Request) { req.Header.Del("x-tenant-id"); req.Header.Del("x-user-id") },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			obstest.Init(t)
			w := newGwBWiring()
			r := w.router()
			req := gwBMintRequest(t)
			mutate(req)
			beforeAll := obstest.Sum(t, gwBMintedMetric, nil)

			rec := w.serve(r, req)

			assert.Equal(t, http.StatusUnauthorized, rec.Code, rec.Body.String())
			assert.Empty(t, w.tickets.allows)
			assert.Empty(t, w.tickets.puts)
			assert.Equal(t, beforeAll, obstest.Sum(t, gwBMintedMetric, nil))
		})
	}
}

// A mint carrying plan, flags, roles and departments stores a snapshot with every one of them,
// stamped with a UTC issue time, and answers 201 with the token the snapshot is stored under.
func TestGwB_TicketMint_TheStoredSnapshotCarriesEveryHeaderAndAnIssueTime(t *testing.T) {
	obstest.Init(t)
	w := newGwBWiring()
	r := w.router()
	req := gwBMintRequest(t)
	req.Header.Set("x-tenant-roles", "tenant_admin,reviewer")
	req.Header.Set("x-departments", testDeptA+":approver, "+testDeptB+":reviewer")
	req.Header.Set("x-plan", "enterprise")
	req.Header.Set("x-feature-flags", "beta, alpha")
	labels := map[string]string{"result": "ok"}
	before := obstest.Sum(t, gwBMintedMetric, labels)
	start := time.Now()

	rec := w.serve(r, req)
	end := time.Now()

	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.Len(t, w.tickets.puts, 1)
	put := w.tickets.puts[0]
	assert.JSONEq(t, `{"ticket":"`+put.token+`","expires_in":60}`, rec.Body.String())
	assert.Equal(t, 60*time.Second, put.ttl)
	assert.Equal(t, []gwBAllow{{tenant: testTenantID, user: testUserID, max: 10, window: time.Minute}}, w.tickets.allows)
	assert.Equal(t, before+1, obstest.Sum(t, gwBMintedMetric, labels))

	ticket := put.ticket
	assert.Equal(t, testTenantID, ticket.TenantID)
	assert.Equal(t, testUserID, ticket.UserID)
	assert.Equal(t, []string{"tenant_admin", "reviewer"}, ticket.TenantRoles)
	assert.Equal(t, []domain.DepartmentRole{
		{DepartmentID: testDeptA, Role: "approver"},
		{DepartmentID: testDeptB, Role: "reviewer"},
	}, ticket.Departments)
	assert.Equal(t, "enterprise", ticket.Plan)
	assert.Equal(t, []string{"beta", "alpha"}, ticket.FeatureFlags)
	assert.Equal(t, time.UTC, ticket.IssuedAt.Location())
	assert.False(t, ticket.IssuedAt.Before(start), "issued no earlier than the request")
	assert.False(t, ticket.IssuedAt.After(end), "issued no later than the response")
}
