//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func blockedReads(t *testing.T, client *redis.Client) int {
	t.Helper()
	list, err := client.ClientList(context.Background()).Result()
	require.NoError(t, err)
	n := 0
	for _, line := range strings.Split(list, "\n") {
		if strings.Contains(line, "cmd=xread") && strings.Contains(line, "flags=b") {
			n++
		}
	}
	return n
}

func TestRun_OpenStreamsDoNotStarveEventIngest(t *testing.T) {
	addr := startValkey(t)
	httpPort := freePort(t)
	setServerEnv(t, addr, httpPort, freePort(t))
	t.Setenv("VALKEY_POOL_SIZE", "2")
	t.Setenv("HEARTBEAT_INTERVAL", "10s")
	cfg, log := loadServerConfig(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg, log) }()
	defer func() { cancel(); <-done }()
	base := fmt.Sprintf("http://127.0.0.1:%d", httpPort)
	require.Eventually(t, func() bool {
		resp, err := http.Get(base + "/readyz")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 30*time.Second, 20*time.Millisecond, "the server becomes ready")

	observer := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = observer.Close() })
	streamCtx, stopStreams := context.WithCancel(context.Background())
	defer stopStreams()
	for i := range 2 {
		mint, err := http.NewRequest(http.MethodPost, base+"/api/v1/stream/tickets", nil)
		require.NoError(t, err)
		mint.Header.Set("x-tenant-id", "00000000-0000-4000-8000-0000000000a1")
		mint.Header.Set("x-user-id", fmt.Sprintf("00000000-0000-4000-8000-0000000000b%d", i))
		mint.Header.Set("x-tenant-roles", "member")
		resp, err := http.DefaultClient.Do(mint)
		require.NoError(t, err)
		var minted struct {
			Ticket string `json:"ticket"`
		}
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&minted))
		_ = resp.Body.Close()
		req, err := http.NewRequestWithContext(streamCtx, http.MethodGet, base+"/api/v1/stream?ticket="+minted.Ticket, nil)
		require.NoError(t, err)
		go func() {
			if resp, err := http.DefaultClient.Do(req); err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		}()
	}
	require.Eventually(t, func() bool { return blockedReads(t, observer) == 2 }, 30*time.Second, 5*time.Millisecond, "both streams wait in a blocking read")

	envelope := `{"id":"0190c6a2-7b3e-7f00-9a1b-2c3d4e5f6a70","type":"WorkflowTaskCreated","source":"workflow-execution-svc","specversion":"1",` +
		`"tenant_id":"00000000-0000-4000-8000-0000000000a1","time":"2026-11-25T10:00:00Z",` +
		`"data":{"recipients":[{"kind":"user","id":"00000000-0000-4000-8000-0000000000c1"}]}}`
	ingest, err := http.NewRequest(http.MethodPost, base+"/api/v1/internal/events", strings.NewReader(envelope))
	require.NoError(t, err)
	ingest.Header.Set("Content-Type", "application/json")
	ingest.Header.Set("x-user-id", "iam-system")
	ingest.Header.Set("x-tenant-roles", "iam-system")
	resp, err := http.DefaultClient.Do(ingest)
	require.NoError(t, err)
	_ = resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode, "ingest has connections of its own while streams hold theirs")
}
