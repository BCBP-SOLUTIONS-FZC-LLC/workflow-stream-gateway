package middleware

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability"
)

type problemDetails struct {
	Type      string `json:"type"`
	Title     string `json:"title"`
	Status    int    `json:"status"`
	Detail    string `json:"detail"`
	Instance  string `json:"instance"`
	Code      string `json:"code"`
	TraceID   string `json:"trace_id,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

func RequireSystemCaller() gin.HandlerFunc {
	return func(c *gin.Context) {
		if reason := rejectionReason(c); reason != "" {
			recordRejection(reason)
			c.Header("Content-Type", "application/problem+json")
			c.AbortWithStatusJSON(http.StatusUnauthorized, problemDetails{
				Type:      "https://errors.bcbp.io/workflow-stream-gateway/unauthorized",
				Title:     "Unauthorized",
				Status:    http.StatusUnauthorized,
				Detail:    "missing or invalid internal caller identity",
				Instance:  c.Request.URL.Path,
				Code:      "unauthorized",
				TraceID:   c.Writer.Header().Get("X-Trace-ID"),
				RequestID: c.Writer.Header().Get("X-Request-ID"),
			})
			return
		}
		c.Next()
	}
}

func recordRejection(reason string) {
	if observability.InternalAuthRejectedTotal != nil {
		observability.InternalAuthRejectedTotal.WithLabelValues(reason).Inc()
	}
}

func rejectionReason(c *gin.Context) string {
	switch {
	case strings.TrimSpace(c.GetHeader("x-user-id")) == "":
		return "missing_user"
	case !hasSystemRole(c.GetHeader("x-tenant-roles")):
		return "missing_role"
	}
	return ""
}

func hasSystemRole(roles string) bool {
	for _, r := range strings.Split(roles, ",") {
		if strings.TrimSpace(r) == domain.SystemCaller {
			return true
		}
	}
	return false
}

const maxBodyBytes = 10 << 20

func LimitRequestBody() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBodyBytes)
		c.Next()
	}
}
