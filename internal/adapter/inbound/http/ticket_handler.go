package http

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

const (
	headerDepartments  = "x-departments"
	headerPlan         = "x-plan"
	headerFeatureFlags = "x-feature-flags"
)

// TicketHandler implements POST /api/v1/stream/tickets (LLD §5.1).
type TicketHandler struct {
	tickets *usecase.TicketService
}

func NewTicketHandler(tickets *usecase.TicketService) *TicketHandler {
	return &TicketHandler{tickets: tickets}
}

func (h *TicketHandler) Mint(c *gin.Context) {
	rc, ok := gincommon.RequestContext(c)
	if !ok || rc.TenantID == "" || rc.UserID == "" {
		writeProblem(c, http.StatusUnauthorized, "unauthorized", "missing gateway identity headers")
		return
	}

	snapshot := domain.Ticket{
		TenantID:     rc.TenantID,
		UserID:       rc.UserID,
		TenantRoles:  rc.Roles,
		Departments:  splitHeaderList(c.GetHeader(headerDepartments)),
		Plan:         c.GetHeader(headerPlan),
		FeatureFlags: splitHeaderList(c.GetHeader(headerFeatureFlags)),
		IssuedAt:     time.Now().UTC(),
	}

	token, ttl, err := h.tickets.Mint(c.Request.Context(), snapshot)
	switch {
	case errors.Is(err, usecase.ErrRateLimited):
		writeProblem(c, http.StatusTooManyRequests, "rate-limited", "ticket mint rate limit exceeded")
		return
	case err != nil:
		writeProblem(c, http.StatusServiceUnavailable, "unavailable", "ticket store unavailable")
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"ticket":     token,
		"expires_in": int(ttl.Seconds()),
	})
}

// splitHeaderList mirrors execution_service's departmentsHeader parsing
// convention: comma-separated, trimmed, empty entries dropped.
func splitHeaderList(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
