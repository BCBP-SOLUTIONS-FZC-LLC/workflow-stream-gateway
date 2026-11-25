package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/config"
)

type gwDLogEntry struct {
	level  string
	msg    string
	fields map[string]interface{}
}

// gwDRecordingLogger keeps every call so a test can assert the message and fields.
type gwDRecordingLogger struct {
	mu      sync.Mutex
	entries []gwDLogEntry
	onLog   func(level, msg string)
}

func (l *gwDRecordingLogger) add(level, msg string, fields map[string]interface{}) {
	l.mu.Lock()
	l.entries = append(l.entries, gwDLogEntry{level, msg, fields})
	hook := l.onLog
	l.mu.Unlock()
	if hook != nil {
		hook(level, msg)
	}
}

func (l *gwDRecordingLogger) Debug(msg string, f map[string]interface{}) { l.add("debug", msg, f) }
func (l *gwDRecordingLogger) Info(msg string, f map[string]interface{})  { l.add("info", msg, f) }
func (l *gwDRecordingLogger) Warn(msg string, f map[string]interface{})  { l.add("warn", msg, f) }
func (l *gwDRecordingLogger) Error(msg string, f map[string]interface{}) { l.add("error", msg, f) }

func (l *gwDRecordingLogger) snapshot() []gwDLogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]gwDLogEntry(nil), l.entries...)
}

func (l *gwDRecordingLogger) messages() []string {
	var out []string
	for _, e := range l.snapshot() {
		out = append(out, e.msg)
	}
	return out
}

// gwDClearServerEnv sets every variable the server reads to the empty string (the default), so a
// machine setting cannot change a test.
func gwDClearServerEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"APP_ENV", "BUILD_VERSION", "OTEL_SERVICE_NAME", "HTTP_PORT", "METRICS_PORT", "ROUTING_CONFIG_PATH",
		"CALLER_IDENTITY_CHECK", "VALKEY_ADDR", "VALKEY_PASSWORD", "VALKEY_DIAL_TIMEOUT", "VALKEY_READ_TIMEOUT",
		"VALKEY_WRITE_TIMEOUT", "TICKET_TTL", "TICKET_MINT_RATE_MAX", "TICKET_MINT_RATE_WINDOW",
		"STREAM_RETENTION_WINDOW", "STREAM_KEY_TTL", "CONNECTION_CAP_PER_USER", "HEARTBEAT_INTERVAL",
		"LOG_FILE", "LOG_LEVEL",
	} {
		t.Setenv(key, "")
	}
}

// gwDHasPendingConnection reports whether a client other than this helper connected to l before
// the call. A connection that completed the TCP handshake waits in the listener's queue in order,
// so the helper connects itself and checks that the first connection in the queue is its own.
func gwDHasPendingConnection(t *testing.T, l net.Listener) bool {
	t.Helper()
	tcp, ok := l.(*net.TCPListener)
	require.True(t, ok)
	sentinel, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)
	defer func() { _ = sentinel.Close() }()
	require.NoError(t, tcp.SetDeadline(time.Now().Add(30*time.Second)), "a safety bound only; the sentinel is already queued")
	first, err := tcp.Accept()
	require.NoError(t, err)
	defer func() { _ = first.Close() }()
	return first.RemoteAddr().String() != sentinel.LocalAddr().String()
}

func gwDListener(t *testing.T) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func TestGwD_PendingConnectionProbe_SeesADialAndAnIdleListener(t *testing.T) {
	idle := gwDListener(t)
	assert.False(t, gwDHasPendingConnection(t, idle))

	dialed := gwDListener(t)
	conn, err := net.Dial("tcp", dialed.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	assert.True(t, gwDHasPendingConnection(t, dialed))
}

func TestGwD_Serve_AFailedStartIsLoggedAsFatalWithTheErrorField(t *testing.T) {
	gwDClearServerEnv(t)
	logFile := filepath.Join(t.TempDir(), "gateway.log")
	missing := filepath.Join(t.TempDir(), "absent-routing.yaml")
	t.Setenv("APP_ENV", "test")
	t.Setenv("BUILD_VERSION", "test")
	t.Setenv("LOG_FILE", logFile)
	t.Setenv("ROUTING_CONFIG_PATH", missing)
	var stderr bytes.Buffer

	code := serve(context.Background(), &stderr)

	assert.Equal(t, 1, code)
	assert.Empty(t, stderr.String(), "the start-up error goes to the logger, not to the early-failure stream")
	raw, err := os.ReadFile(logFile)
	require.NoError(t, err)
	var fatal []map[string]interface{}
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		var line map[string]interface{}
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &line), scanner.Text())
		if line["msg"] == "fatal" {
			fatal = append(fatal, line)
		}
	}
	require.NoError(t, scanner.Err())
	require.Len(t, fatal, 1, "exactly one fatal entry")
	assert.Equal(t, "error", fatal[0]["level"])
	message, ok := fatal[0]["error"].(string)
	require.True(t, ok, "the entry carries an error field holding the message")
	assert.Contains(t, message, "routing: reading "+missing+": ")
	assert.Contains(t, message, "no such file or directory")
}

func TestGwD_Run_ARoutingFileThatCannotBeLoadedFailsBeforeValkeyIsDialled(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) string {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
		return path
	}
	cases := []struct {
		name    string
		path    string
		wantErr string
		wantIs  error
	}{
		{"path does not exist", filepath.Join(dir, "absent.yaml"), "routing: reading ", fs.ErrNotExist},
		{"empty file", write("empty.yaml", ""), " is empty", nil},
		{"not yaml for the format", write("broken.yaml", "version: [1\n"), "routing: parsing ", nil},
		{"unsupported version", write("version.yaml", "version: 2\nservices: {}\n"), "version must be 1, got 2", nil},
		{"unknown field", write("unknown.yaml", "version: 1\nservices: {}\nextra: 1\n"), "routing: parsing ", nil},
		{"no services", write("none.yaml", "version: 1\nservices: {}\n"), "at least one service is required", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			valkeyStandIn := gwDListener(t)
			log := &gwDRecordingLogger{}
			cfg := &config.Config{
				RoutingConfigPath: tc.path,
				ValkeyAddr:        valkeyStandIn.Addr().String(),
				ValkeyDialTimeout: time.Second,
				HTTPPort:          1,
				MetricsPort:       1,
			}

			err := run(context.Background(), cfg, log)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
			assert.Contains(t, err.Error(), tc.path)
			if tc.wantIs != nil {
				assert.True(t, errors.Is(err, tc.wantIs))
			}
			assert.False(t, gwDHasPendingConnection(t, valkeyStandIn), "no connection to Valkey was attempted")
			assert.Empty(t, log.messages(), "nothing was started, so nothing was logged")
		})
	}
}
