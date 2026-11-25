//go:build integration

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func gwDReachable(port int) bool {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// A port that is taken makes run return the listener's error, and the error names the port that
// failed, whichever of the two listeners it was. The other port is free, so the message cannot
// be about it.
func TestGwD_Run_ABusyPortFailsNamingThatPort(t *testing.T) {
	valkeyAddr := startValkey(t)
	for _, busy := range []string{"http", "metrics"} {
		t.Run(busy, func(t *testing.T) {
			held, err := net.Listen("tcp", ":0")
			require.NoError(t, err)
			t.Cleanup(func() { _ = held.Close() })
			busyPort := held.Addr().(*net.TCPAddr).Port
			freeServerPort := freePort(t)
			httpPort, metricsPort := busyPort, freeServerPort
			if busy == "metrics" {
				httpPort, metricsPort = freeServerPort, busyPort
			}
			setServerEnv(t, valkeyAddr, httpPort, metricsPort)
			cfg, _ := loadServerConfig(t)
			log := &gwDRecordingLogger{}

			err = run(context.Background(), cfg, log)

			require.Error(t, err)
			assert.Contains(t, err.Error(), fmt.Sprintf(":%d: bind: address already in use", busyPort))
			assert.NotContains(t, err.Error(), fmt.Sprintf(":%d:", freeServerPort), "the error is about the busy port only")
		})
	}
}

// On a termination signal run logs "shutting down" first, while both listeners still accept, then
// closes both and returns no error.
func TestGwD_Run_ShutdownLogsBeforeItClosesBothListeners(t *testing.T) {
	httpPort, metricsPort := freePort(t), freePort(t)
	setServerEnv(t, startValkey(t), httpPort, metricsPort)
	cfg, _ := loadServerConfig(t)
	log := &gwDRecordingLogger{}
	var httpOpenAtLog, metricsOpenAtLog bool
	log.onLog = func(_, msg string) {
		if msg == "shutting down" {
			httpOpenAtLog, metricsOpenAtLog = gwDReachable(httpPort), gwDReachable(metricsPort)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg, log) }()
	require.Eventually(t, func() bool {
		for _, url := range []string{
			fmt.Sprintf("http://127.0.0.1:%d/readyz", httpPort),
			fmt.Sprintf("http://127.0.0.1:%d/metrics", metricsPort),
		} {
			resp, err := http.Get(url)
			if err != nil {
				return false
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return false
			}
		}
		return true
	}, 30*time.Second, 20*time.Millisecond, "both listeners are serving")

	cancel()

	require.NoError(t, <-done, "both servers shut down within the shutdown timeout")
	assert.True(t, httpOpenAtLog, "the log line comes before the HTTP server stops accepting")
	assert.True(t, metricsOpenAtLog, "the log line comes before the metrics server stops accepting")
	assert.False(t, gwDReachable(httpPort), "the HTTP listener is closed when run returns")
	assert.False(t, gwDReachable(metricsPort), "the metrics listener is closed when run returns")

	messages := log.messages()
	shuttingDown := slices.Index(messages, "shutting down")
	require.NotEqual(t, -1, shuttingDown, "run logs 'shutting down'")
	assert.Equal(t, 1, gwDCountOf(messages, "shutting down"))
	listening, metricsListening := slices.Index(messages, "listening"), slices.Index(messages, "metrics listening")
	require.NotEqual(t, -1, listening)
	require.NotEqual(t, -1, metricsListening)
	assert.Less(t, listening, shuttingDown)
	assert.Less(t, metricsListening, shuttingDown)
	for _, entry := range log.snapshot() {
		assert.NotEqual(t, "error", entry.level, "no error is logged on a clean shutdown: %s", entry.msg)
		if entry.msg == "listening" {
			assert.Equal(t, httpPort, entry.fields["port"])
		}
		if entry.msg == "metrics listening" {
			assert.Equal(t, metricsPort, entry.fields["port"])
		}
	}
}

func gwDCountOf(list []string, want string) int {
	n := 0
	for _, s := range list {
		if s == want {
			n++
		}
	}
	return n
}
