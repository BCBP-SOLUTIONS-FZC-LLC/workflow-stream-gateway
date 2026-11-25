package http

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

func TestTicketHandler_Mint_FeatureFlagsHeader(t *testing.T) {
	cases := []struct {
		name   string
		header string
		want   []string
	}{
		{name: "empty header gives no flags", header: "", want: nil},
		{name: "only commas and spaces gives no flags", header: " , ,,  ", want: []string{}},
		{name: "blank items dropped and names trimmed", header: "beta, ,alpha ,,", want: []string{"beta", "alpha"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &fixtureStore{tickets: map[string]domain.Ticket{}, allow: true}
			r := newTestRouter(usecase.NewTicketService(store, 60*time.Second, 10, time.Minute))

			req := httptest.NewRequest(http.MethodPost, "/api/v1/stream/tickets", nil)
			req.Header.Set("x-user-id", testUserID)
			req.Header.Set("x-tenant-id", testTenantID)
			if tc.header != "" {
				req.Header.Set("x-feature-flags", tc.header)
			}
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)

			require.Equal(t, http.StatusCreated, rec.Code)
			require.Len(t, store.tickets, 1)
			for _, ticket := range store.tickets {
				assert.Equal(t, tc.want, ticket.FeatureFlags)
			}
		})
	}
}

func TestTicketHandler_Mint_MetricsNotInitialised_StillCreated(t *testing.T) {
	saved := observability.TicketsMintedTotal
	observability.TicketsMintedTotal = nil
	t.Cleanup(func() { observability.TicketsMintedTotal = saved })

	store := &fixtureStore{tickets: map[string]domain.Ticket{}, allow: true}
	r := newTestRouter(usecase.NewTicketService(store, 60*time.Second, 10, time.Minute))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/stream/tickets", nil)
	req.Header.Set("x-user-id", testUserID)
	req.Header.Set("x-tenant-id", testTenantID)
	rec := httptest.NewRecorder()

	require.NotPanics(t, func() { r.ServeHTTP(rec, req) })
	assert.Equal(t, http.StatusCreated, rec.Code)
	assert.Contains(t, rec.Body.String(), `"ticket"`)
	assert.Len(t, store.tickets, 1)
}
