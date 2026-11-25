package middleware

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLimitRequestBody_BodySize_ReadsInFullOrRejects(t *testing.T) {
	cases := []struct {
		name       string
		size       int
		wantStatus int
		wantRead   int
	}{
		{"exactly the limit is read in full", maxBodyBytes, http.StatusOK, maxBodyBytes},
		{"one byte over the limit is rejected", maxBodyBytes + 1, http.StatusRequestEntityTooLarge, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gin.SetMode(gin.TestMode)
			r := gin.New()
			r.Use(LimitRequestBody())
			r.POST("/internal/events", func(c *gin.Context) {
				b, err := io.ReadAll(c.Request.Body)
				var tooLarge *http.MaxBytesError
				if errors.As(err, &tooLarge) {
					c.String(http.StatusRequestEntityTooLarge, strconv.FormatInt(tooLarge.Limit, 10))
					return
				}
				require.NoError(t, err)
				c.String(http.StatusOK, strconv.Itoa(len(b)))
			})

			req := httptest.NewRequest(http.MethodPost, "/internal/events", bytes.NewReader(make([]byte, tc.size)))
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			assert.Equal(t, tc.wantStatus, w.Code)
			if tc.wantStatus == http.StatusOK {
				assert.Equal(t, strconv.Itoa(tc.wantRead), w.Body.String())
			} else {
				assert.Equal(t, strconv.Itoa(maxBodyBytes), w.Body.String())
			}
		})
	}
}
