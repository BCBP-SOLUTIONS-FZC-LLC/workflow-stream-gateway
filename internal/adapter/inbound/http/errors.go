package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// problemDetails is this repo's own RFC-9457 body — no shared package
// exists in platform-libs (confirmed across execution_service and
// definition_service, which each carry an equivalent, independent copy).
type problemDetails struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail"`
	Instance string `json:"instance"`
	Code     string `json:"code"`
}

func writeProblem(c *gin.Context, status int, code, detail string) {
	c.Header("Content-Type", "application/problem+json")
	c.AbortWithStatusJSON(status, problemDetails{
		Type:     "https://errors.bcbp.io/workflow-stream-gateway/" + code,
		Title:    http.StatusText(status),
		Status:   status,
		Detail:   detail,
		Instance: c.Request.URL.Path,
		Code:     code,
	})
}
