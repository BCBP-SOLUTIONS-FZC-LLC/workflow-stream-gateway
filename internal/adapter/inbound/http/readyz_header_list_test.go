package http

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readyzPongServerAddr starts a minimal TCP server that answers every read
// with a RESP simple string "+PONG", enough for a Ping with protocol 2.
func readyzPongServerAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				buf := make([]byte, 512)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					reply := "+PONG\r\n"
					if strings.Contains(strings.ToUpper(string(buf[:n])), "HELLO") {
						reply = "-ERR unknown command\r\n"
					}
					if _, err := c.Write([]byte(reply)); err != nil {
						return
					}
				}
			}(conn)
		}
	}()
	return ln.Addr().String()
}

func TestNewRouter_Readyz_ValkeyPingOk_Returns200WithEmptyBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	client := redis.NewClient(&redis.Options{
		Addr:                     readyzPongServerAddr(t),
		Protocol:                 2,
		DisableIdentity:          true,
		MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
		DialTimeout:              2 * time.Second,
		ReadTimeout:              2 * time.Second,
	})
	t.Cleanup(func() { _ = client.Close() })
	cfg := testRouterConfig()
	cfg.ValkeyClient = client
	r := NewRouter(cfg)

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, w.Body.String())
}

func TestSplitHeaderList_Input_TrimsAndDropsBlanks(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		raw  string
		want []string
	}{
		{"empty header gives nil", "", nil},
		{"trims spaces around items", " a , b ,c", []string{"a", "b", "c"}},
		{"drops blank items", "a,, ,b,", []string{"a", "b"}},
		{"only blanks gives empty non-nil", " , ,", []string{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, splitHeaderList(tc.raw))
		})
	}
}
