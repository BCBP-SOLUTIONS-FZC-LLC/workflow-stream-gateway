package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

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
