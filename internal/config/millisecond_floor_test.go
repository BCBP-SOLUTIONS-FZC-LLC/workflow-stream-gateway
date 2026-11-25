package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_RefusesADurationBelowOneMillisecond(t *testing.T) {
	for _, key := range []string{"TICKET_TTL", "TICKET_MINT_RATE_WINDOW", "STREAM_RETENTION_WINDOW", "HEARTBEAT_INTERVAL"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv("APP_ENV", "test")
			t.Setenv("CALLER_IDENTITY_CHECK", "false")
			t.Setenv(key, "500us")

			_, err := Load()

			require.Error(t, err, "Valkey takes these in whole milliseconds; below one they become 0, which means no expiry, no window or a read that never returns")
			assert.Contains(t, err.Error(), key+" must be at least 1ms")
		})
	}
}
