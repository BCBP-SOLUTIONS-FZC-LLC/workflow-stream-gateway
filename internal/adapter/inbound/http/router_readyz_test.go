package http

import (
	"bufio"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readyzFakeValkey is a minimal RESP server: it answers PING with PONG and
// refuses the RESP3 handshake so the client falls back to RESP2. While
// healthy is false it drops every connection.
type readyzFakeValkey struct {
	ln      net.Listener
	healthy atomic.Bool
}

func newReadyzFakeValkey(t *testing.T) *readyzFakeValkey {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	f := &readyzFakeValkey{ln: ln}
	f.healthy.Store(true)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(conn)
		}
	}()
	return f
}

func (f *readyzFakeValkey) serve(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	br := bufio.NewReader(conn)
	for {
		line, err := br.ReadString('\n')
		if err != nil || !strings.HasPrefix(line, "*") {
			return
		}
		n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil {
			return
		}
		var args []string
		for i := 0; i < n; i++ {
			if _, err := br.ReadString('\n'); err != nil { // $len
				return
			}
			arg, err := br.ReadString('\n')
			if err != nil {
				return
			}
			args = append(args, strings.TrimSpace(arg))
		}
		if !f.healthy.Load() {
			return
		}
		reply := "+OK\r\n"
		switch strings.ToUpper(args[0]) {
		case "PING":
			reply = "+PONG\r\n"
		case "HELLO":
			reply = "-ERR unknown command\r\n"
		}
		if _, err := conn.Write([]byte(reply)); err != nil {
			return
		}
	}
}

func readyzRouterFor(f *readyzFakeValkey) *gin.Engine {
	gin.SetMode(gin.TestMode)
	cfg := testRouterConfig()
	cfg.ValkeyClient = redis.NewClient(&redis.Options{Addr: f.ln.Addr().String(), MaxRetries: -1})
	return NewRouter(cfg)
}

func TestNewRouter_Readyz_ValkeyReachable_Returns200(t *testing.T) {
	t.Parallel()
	r := readyzRouterFor(newReadyzFakeValkey(t))

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, w.Body.String())
}

func TestNewRouter_Readyz_ValkeyRecoversAfterOutage_Returns200(t *testing.T) {
	t.Parallel()
	f := newReadyzFakeValkey(t)
	r := readyzRouterFor(f)
	get := func() *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		return w
	}

	require.Equal(t, http.StatusOK, get().Code)

	f.healthy.Store(false)
	down := get()
	assert.Equal(t, http.StatusServiceUnavailable, down.Code)
	assert.Empty(t, down.Body.String())

	f.healthy.Store(true)
	up := get()
	assert.Equal(t, http.StatusOK, up.Code)
	assert.Empty(t, up.Body.String())
}

func TestNewRouter_Readyz_RequestContextCancelled_Returns503(t *testing.T) {
	t.Parallel()
	r := readyzRouterFor(newReadyzFakeValkey(t))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil).WithContext(ctx)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Empty(t, w.Body.String())
}
