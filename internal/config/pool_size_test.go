package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_ValkeyPoolSizes(t *testing.T) {
	t.Setenv("APP_ENV", "test")
	t.Setenv("CALLER_IDENTITY_CHECK", "false")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, 20, cfg.ValkeyPoolSize)
	assert.Equal(t, 1000, cfg.ValkeyStreamPoolSize)

	for _, key := range []string{"VALKEY_POOL_SIZE", "VALKEY_STREAM_POOL_SIZE"} {
		for _, value := range []string{"0", "-1"} {
			t.Run(key+"="+value, func(t *testing.T) {
				t.Setenv(key, value)

				_, err := Load()

				assert.EqualError(t, err, key+" must be > 0")
			})
		}
	}
}
