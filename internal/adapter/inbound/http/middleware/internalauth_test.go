package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

func newTestEngine(token string) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(RequireInternalToken(token))
	r.POST("/internal/events", func(c *gin.Context) { c.Status(http.StatusOK) })
	return r
}

func TestRequireInternalToken_EmptyTokenDisablesCheck(t *testing.T) {
	r := newTestEngine("")

	req := httptest.NewRequest(http.MethodPost, "/internal/events", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestRequireInternalToken_CorrectTokenPasses(t *testing.T) {
	r := newTestEngine("secret")

	req := httptest.NewRequest(http.MethodPost, "/internal/events", nil)
	req.Header.Set(InternalTokenHeader, "secret")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestRequireInternalToken_MissingTokenRejected(t *testing.T) {
	r := newTestEngine("secret")

	req := httptest.NewRequest(http.MethodPost, "/internal/events", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestRequireInternalToken_WrongTokenRejected(t *testing.T) {
	r := newTestEngine("secret")

	req := httptest.NewRequest(http.MethodPost, "/internal/events", nil)
	req.Header.Set(InternalTokenHeader, "wrong")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}
