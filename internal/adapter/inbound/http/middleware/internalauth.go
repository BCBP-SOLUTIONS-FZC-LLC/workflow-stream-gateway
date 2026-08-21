// Package middleware holds gin middlewares that don't belong to a single
// handler — currently just the internal-route-group token check.
package middleware

import (
	"crypto/subtle"
	"net/http"

	"github.com/gin-gonic/gin"
)

const InternalTokenHeader = "x-internal-token"

// problemDetails is a minimal, local RFC-9457 body — kept independent of
// the handler package's own (unexported) writeProblem rather than
// importing it, so this middleware has no dependency on a specific
// adapter package (mirrors execution_service's identical middleware).
type problemDetails struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail"`
	Instance string `json:"instance"`
	Code     string `json:"code"`
}

// RequireInternalToken guards the /internal route group (LLD §3.2). When
// token is empty the check is disabled (local/dev) and requests pass
// through; NetworkPolicy remains the primary control. When token is set,
// a request missing or mismatching the x-internal-token header is
// rejected with 401 and an RFC-9457 problem-details body. The comparison
// is constant-time to avoid a timing side-channel on the shared secret.
func RequireInternalToken(token string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if token == "" {
			c.Next()
			return
		}
		provided := c.GetHeader(InternalTokenHeader)
		if subtle.ConstantTimeCompare([]byte(provided), []byte(token)) != 1 {
			c.Header("Content-Type", "application/problem+json")
			c.AbortWithStatusJSON(http.StatusUnauthorized, problemDetails{
				Type:     "https://errors.bcbp.io/workflow-stream-gateway/unauthorized",
				Title:    "Unauthorized",
				Status:   http.StatusUnauthorized,
				Detail:   "missing or invalid x-internal-token header",
				Instance: c.Request.URL.Path,
				Code:     "UNAUTHORIZED",
			})
			return
		}
		c.Next()
	}
}
