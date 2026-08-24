package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

// Registering metrics once for this package's whole test suite lets every
// handler test exercise its real observability.*.Inc() call sites, not
// just the nil-guard around them — matching how cmd/server calls this
// exactly once at real process boot.
func init() {
	observability.Register()
}

// fixtureStore is a minimal usecase.Store fake for exercising the HTTP
// handler layer — kept local rather than shared with internal/usecase's
// own fake, since it's small enough that a shared test-only package would
// add more indirection than it saves.
type fixtureStore struct {
	tickets  map[string]domain.Ticket
	allow    bool
	forceErr error
}

func (f *fixtureStore) AllowMint(_ context.Context, _, _ string, _ int, _ time.Duration) (bool, error) {
	if f.forceErr != nil {
		return false, f.forceErr
	}
	return f.allow, nil
}

func (f *fixtureStore) PutTicket(_ context.Context, token string, ticket domain.Ticket, _ time.Duration) error {
	if f.forceErr != nil {
		return f.forceErr
	}
	f.tickets[token] = ticket
	return nil
}

func (f *fixtureStore) GetTicket(_ context.Context, token string) (domain.Ticket, error) {
	if f.forceErr != nil {
		return domain.Ticket{}, f.forceErr
	}
	ticket, ok := f.tickets[token]
	if !ok {
		return domain.Ticket{}, usecase.ErrTicketNotFound
	}
	return ticket, nil
}

func newTestRouter(svc *usecase.TicketService) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	api := r.Group("/api/v1")
	for _, mw := range gincommon.ProtectedMiddlewares(gincommon.Config{ServiceName: "workflow-stream-gateway-test"}) {
		api.Use(mw)
	}
	api.POST("/stream/tickets", NewTicketHandler(svc).Mint)
	return r
}

func TestTicketHandler_Mint_Success(t *testing.T) {
	store := &fixtureStore{tickets: map[string]domain.Ticket{}, allow: true}
	svc := usecase.NewTicketService(store, 60*time.Second, 10, time.Minute)
	r := newTestRouter(svc)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/stream/tickets", nil)
	req.Header.Set("x-user-id", "user-1")
	req.Header.Set("x-tenant-id", "tenant-1")
	req.Header.Set("x-tenant-roles", "admin,user")
	req.Header.Set("x-departments", "dept-a:approver, dept-b:reviewer")
	rec := httptest.NewRecorder()

	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.NotEmpty(t, body["ticket"])
	assert.Equal(t, float64(60), body["expires_in"])

	require.Len(t, store.tickets, 1)
	for _, ticket := range store.tickets {
		assert.Equal(t, "tenant-1", ticket.TenantID)
		assert.Equal(t, "user-1", ticket.UserID)
		assert.Equal(t, []domain.DepartmentRole{
			{DepartmentID: "dept-a", Role: "approver"},
			{DepartmentID: "dept-b", Role: "reviewer"},
		}, ticket.Departments)
	}
}

func TestTicketHandler_Mint_MissingIdentityHeaders(t *testing.T) {
	store := &fixtureStore{tickets: map[string]domain.Ticket{}, allow: true}
	svc := usecase.NewTicketService(store, 60*time.Second, 10, time.Minute)
	r := newTestRouter(svc)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/stream/tickets", nil)
	rec := httptest.NewRecorder()

	r.ServeHTTP(rec, req)

	// gincommon's own RequireAuth middleware already rejects a request
	// with no gateway identity headers before Mint ever runs — this test
	// just confirms 401 either way.
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

// TestTicketHandler_Mint_NoRequestContext exercises the handler's own
// defensive check directly (belt-and-suspenders behind gincommon's
// RequireAuth, matching execution_service's identical precedent) by
// invoking Mint with no middleware chain at all, so gincommon never
// injects a RequestContext.
func TestTicketHandler_Mint_NoRequestContext(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &fixtureStore{tickets: map[string]domain.Ticket{}, allow: true}
	svc := usecase.NewTicketService(store, 60*time.Second, 10, time.Minute)
	handler := NewTicketHandler(svc)

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/stream/tickets", nil)

	handler.Mint(c)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestTicketHandler_Mint_RateLimited(t *testing.T) {
	store := &fixtureStore{tickets: map[string]domain.Ticket{}, allow: false}
	svc := usecase.NewTicketService(store, 60*time.Second, 10, time.Minute)
	r := newTestRouter(svc)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/stream/tickets", nil)
	req.Header.Set("x-user-id", "user-1")
	req.Header.Set("x-tenant-id", "tenant-1")
	rec := httptest.NewRecorder()

	r.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusTooManyRequests, rec.Code)
}

func TestTicketHandler_Mint_StoreUnavailable(t *testing.T) {
	store := &fixtureStore{tickets: map[string]domain.Ticket{}, forceErr: errors.New("valkey unreachable")}
	svc := usecase.NewTicketService(store, 60*time.Second, 10, time.Minute)
	r := newTestRouter(svc)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/stream/tickets", nil)
	req.Header.Set("x-user-id", "user-1")
	req.Header.Set("x-tenant-id", "tenant-1")
	rec := httptest.NewRecorder()

	r.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestSplitDepartments(t *testing.T) {
	assert.Nil(t, splitDepartments(""))
	assert.Equal(t, []domain.DepartmentRole{
		{DepartmentID: "dept-a", Role: "approver"},
	}, splitDepartments("dept-a:approver"))
	assert.Equal(t, []domain.DepartmentRole{
		{DepartmentID: "dept-a", Role: "approver"},
		{DepartmentID: "dept-b", Role: "reviewer"},
	}, splitDepartments("dept-a:approver, dept-b:reviewer"))
	// Missing ":" separator, or an empty ID half, is dropped rather than
	// guessed at (matches execution_service's own fix for the identical
	// wire format).
	assert.Empty(t, splitDepartments("dept-a"))
	assert.Equal(t, []domain.DepartmentRole{
		{DepartmentID: "dept-a", Role: "approver"},
	}, splitDepartments("dept-a:approver, malformed-no-colon, :empty-id"))
}
