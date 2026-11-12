package middleware

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability/obstest"
)

func newTestEngine() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(RequireSystemCaller())
	r.POST("/internal/events", func(c *gin.Context) { c.Status(http.StatusOK) })
	return r
}

func TestRequireSystemCaller(t *testing.T) {
	tests := map[string]struct {
		headers    map[string]string
		wantStatus int
	}{
		"system caller passes":                     {map[string]string{"x-user-id": "iam-system", "x-tenant-roles": "iam-system"}, http.StatusOK},
		"role among several passes":                {map[string]string{"x-user-id": "u", "x-tenant-roles": "tenant_admin, iam-system"}, http.StatusOK},
		"entries are trimmed":                      {map[string]string{"x-user-id": "u", "x-tenant-roles": "  iam-system  ,x"}, http.StatusOK},
		"no headers rejected":                      {nil, http.StatusUnauthorized},
		"missing user id rejected":                 {map[string]string{"x-tenant-roles": "iam-system"}, http.StatusUnauthorized},
		"blank user id rejected":                   {map[string]string{"x-user-id": "   ", "x-tenant-roles": "iam-system"}, http.StatusUnauthorized},
		"missing roles rejected":                   {map[string]string{"x-user-id": "iam-system"}, http.StatusUnauthorized},
		"other role rejected":                      {map[string]string{"x-user-id": "u", "x-tenant-roles": "tenant_admin"}, http.StatusUnauthorized},
		"role match is case sensitive":             {map[string]string{"x-user-id": "u", "x-tenant-roles": "IAM-System"}, http.StatusUnauthorized},
		"role match is exact":                      {map[string]string{"x-user-id": "u", "x-tenant-roles": "iam-system-x,not-iam-system"}, http.StatusUnauthorized},
		"the old token header alone is rejected":   {map[string]string{"x-internal-token": "secret"}, http.StatusUnauthorized},
		"the old token header does not add a role": {map[string]string{"x-user-id": "u", "x-internal-token": "secret"}, http.StatusUnauthorized},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/internal/events", nil)
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}
			rec := httptest.NewRecorder()
			newTestEngine().ServeHTTP(rec, req)

			require.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantStatus != http.StatusUnauthorized {
				return
			}
			assert.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))
			var p struct {
				Code     string `json:"code"`
				Detail   string `json:"detail"`
				Instance string `json:"instance"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &p))
			assert.Equal(t, "unauthorized", p.Code)
			assert.Equal(t, "missing or invalid internal caller identity", p.Detail)
			assert.Equal(t, "/internal/events", p.Instance)
		})
	}
}

func TestRequireSystemCaller_CountsRejectionsByReason(t *testing.T) {
	obstest.Init(t)
	const metric = "workflow_stream_gateway_internal_auth_rejected_total"
	count := func(reason string) float64 { return obstest.Sum(t, metric, map[string]string{"reason": reason}) }
	both := func() float64 { return count("missing_user") + count("missing_role") }

	cases := []struct {
		name    string
		headers map[string]string
		reason  string
	}{
		{"no user id", map[string]string{"x-tenant-roles": "iam-system"}, "missing_user"},
		{"blank user id", map[string]string{"x-user-id": "  ", "x-tenant-roles": "iam-system"}, "missing_user"},
		{"nothing at all names the user first", nil, "missing_user"},
		{"no role", map[string]string{"x-user-id": "u"}, "missing_role"},
		{"another role", map[string]string{"x-user-id": "u", "x-tenant-roles": "tenant_admin"}, "missing_role"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before, otherBefore := count(tc.reason), both()-count(tc.reason)

			req := httptest.NewRequest(http.MethodPost, "/internal/events", nil)
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			rec := httptest.NewRecorder()
			newTestEngine().ServeHTTP(rec, req)

			require.Equal(t, http.StatusUnauthorized, rec.Code)
			assert.Equal(t, before+1, count(tc.reason))
			assert.Equal(t, otherBefore, both()-count(tc.reason), "only the matching reason moves")
		})
	}

	t.Run("an admitted caller is not counted", func(t *testing.T) {
		before := both()

		req := httptest.NewRequest(http.MethodPost, "/internal/events", nil)
		req.Header.Set("x-user-id", "iam-system")
		req.Header.Set("x-tenant-roles", "iam-system")
		rec := httptest.NewRecorder()
		newTestEngine().ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, before, both())
	})
}

func TestSystemCallerIdentity_IsOneConstant(t *testing.T) {
	assert.Equal(t, "iam-system", domain.SystemCaller)
}
