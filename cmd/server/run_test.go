//go:build integration

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/logger"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/config"
)

func startValkey(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "valkey/valkey:8",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor: wait.ForAll(
				wait.ForListeningPort("6379/tcp"),
				wait.ForLog("Ready to accept connections"),
			).WithDeadline(60 * time.Second),
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })
	endpoint, err := container.Endpoint(ctx, "")
	require.NoError(t, err)
	return endpoint
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}

func setServerEnv(t *testing.T, valkeyAddr string, httpPort, metricsPort int) {
	t.Helper()
	t.Setenv("APP_ENV", "test")
	t.Setenv("CALLER_IDENTITY_CHECK", "false")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "none")
	t.Setenv("BUILD_VERSION", "test")
	t.Setenv("VALKEY_ADDR", valkeyAddr)
	t.Setenv("VALKEY_DIAL_TIMEOUT", "500ms")
	t.Setenv("HTTP_PORT", fmt.Sprint(httpPort))
	t.Setenv("METRICS_PORT", fmt.Sprint(metricsPort))
	t.Setenv("HEARTBEAT_INTERVAL", "500ms")
	t.Setenv("ROUTING_CONFIG_PATH", "../../config/routing.yaml")
}

func loadServerConfig(t *testing.T) (*config.Config, Logger) {
	t.Helper()
	cfg, err := config.Load()
	require.NoError(t, err)
	log, err := logger.NewLogger(cfg.AppEnv)
	require.NoError(t, err)
	return cfg, log
}

func startServer(t *testing.T) (base string, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	httpPort, metricsPort := freePort(t), freePort(t)
	setServerEnv(t, startValkey(t), httpPort, metricsPort)
	cfg, log := loadServerConfig(t)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- run(ctx, cfg, log) }()
	base = fmt.Sprintf("http://127.0.0.1:%d", httpPort)
	require.Eventually(t, func() bool {
		resp, err := http.Get(base + "/readyz")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 30*time.Second, 20*time.Millisecond, "the server becomes ready")
	return base, cancel, result
}

func TestRun_AnOpenStreamEndsWhenShutdownStarts(t *testing.T) {
	base, cancel, done := startServer(t)
	defer cancel()

	mint, err := http.NewRequest(http.MethodPost, base+"/api/v1/stream/tickets", nil)
	require.NoError(t, err)
	mint.Header.Set("x-tenant-id", "00000000-0000-4000-8000-0000000000a1")
	mint.Header.Set("x-user-id", "00000000-0000-4000-8000-0000000000b1")
	mint.Header.Set("x-tenant-roles", "member")
	resp, err := http.DefaultClient.Do(mint)
	require.NoError(t, err)
	var minted struct {
		Ticket string `json:"ticket"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&minted))
	_ = resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	stream, err := http.Get(base + "/api/v1/stream?ticket=" + minted.Ticket)
	require.NoError(t, err)
	defer func() { _ = stream.Body.Close() }()
	require.Equal(t, http.StatusOK, stream.StatusCode)
	reader := bufio.NewReader(stream.Body)
	_, err = reader.ReadString('\n')
	require.NoError(t, err, "the stream is open and has sent its first heartbeat")

	cancel()

	require.NoError(t, <-done, "shutdown completes although a stream was open")
	_, err = reader.ReadString(0)
	assert.Error(t, err, "the stream is closed by the server")
}

func TestRun_FailsWhenTheRoutingFileIsMissing(t *testing.T) {
	setServerEnv(t, "127.0.0.1:1", freePort(t), freePort(t))
	t.Setenv("ROUTING_CONFIG_PATH", t.TempDir()+"/absent.yaml")
	cfg, log := loadServerConfig(t)

	assert.Error(t, run(context.Background(), cfg, log))
}

func TestRun_FailsWhenValkeyIsUnreachable(t *testing.T) {
	setServerEnv(t, "127.0.0.1:1", freePort(t), freePort(t))
	cfg, log := loadServerConfig(t)

	err := run(context.Background(), cfg, log)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "valkey ping")
}

func TestServe_ExitsWithOneOnABadSetting(t *testing.T) {
	setServerEnv(t, "127.0.0.1:1", freePort(t), freePort(t))
	t.Setenv("APP_ENV", "nowhere")
	var stderr strings.Builder

	assert.Equal(t, 1, serve(context.Background(), &stderr))
	assert.Contains(t, stderr.String(), "fatal: APP_ENV must be one of")
}

func TestServe_ExitsWithOneWhenStartUpFails(t *testing.T) {
	setServerEnv(t, "127.0.0.1:1", freePort(t), freePort(t))

	assert.Equal(t, 1, serve(context.Background(), io.Discard))
}

func TestServe_ExitsWithZeroAfterAGracefulShutdown(t *testing.T) {
	httpPort := freePort(t)
	setServerEnv(t, startValkey(t), httpPort, freePort(t))
	ctx, cancel := context.WithCancel(context.Background())
	code := make(chan int, 1)
	go func() { code <- serve(ctx, io.Discard) }()
	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/readyz", httpPort))
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 30*time.Second, 20*time.Millisecond, "the server becomes ready")

	cancel()

	assert.Equal(t, 0, <-code)
}

func TestRun_FailsWhenAPortCannotBeBound(t *testing.T) {
	port := freePort(t)
	setServerEnv(t, startValkey(t), port, port)
	cfg, log := loadServerConfig(t)

	err := run(context.Background(), cfg, log)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "address already in use")
}
