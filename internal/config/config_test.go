package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_Defaults(t *testing.T) {
	t.Setenv("APP_ENV", "")
	t.Setenv("WORKFLOW_STREAM_GATEWAY_TOKEN", "")

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "dev", cfg.AppEnv)
	assert.Equal(t, 8080, cfg.HTTPPort)
	assert.Equal(t, 9090, cfg.MetricsPort)
	assert.Equal(t, "localhost:6379", cfg.ValkeyAddr)
	assert.Equal(t, 60*time.Second, cfg.TicketTTL)
	assert.Equal(t, 10, cfg.TicketMintRateMax)
	assert.Equal(t, 60*time.Second, cfg.TicketMintRateWindow)
	assert.Equal(t, 10*time.Minute, cfg.StreamRetentionWindow)
	assert.Equal(t, 15*time.Minute, cfg.StreamKeyTTL)
	assert.Equal(t, 5, cfg.ConnectionCapPerUser)
	assert.Equal(t, 15*time.Second, cfg.HeartbeatInterval)
}

func TestLoad_EnvOverrides(t *testing.T) {
	t.Setenv("HTTP_PORT", "9090")
	t.Setenv("VALKEY_ADDR", "valkey.internal:6379")
	t.Setenv("TICKET_TTL", "30s")
	t.Setenv("TICKET_MINT_RATE_MAX", "5")
	t.Setenv("CONNECTION_CAP_PER_USER", "3")

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, 9090, cfg.HTTPPort)
	assert.Equal(t, "valkey.internal:6379", cfg.ValkeyAddr)
	assert.Equal(t, 30*time.Second, cfg.TicketTTL)
	assert.Equal(t, 5, cfg.TicketMintRateMax)
	assert.Equal(t, 3, cfg.ConnectionCapPerUser)
}

func TestLoad_MalformedValuesFailFast(t *testing.T) {
	for name, env := range map[string][2]string{
		"int":      {"HTTP_PORT", "not-a-number"},
		"duration": {"TICKET_TTL", "soon"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(env[0], env[1])
			_, err := Load()
			assert.ErrorContains(t, err, env[0])
		})
	}
}

func TestLoad_RejectsUnknownAppEnv(t *testing.T) {
	for _, env := range []string{"production", "development", "qa"} {
		t.Setenv("APP_ENV", env)
		_, err := Load()
		assert.ErrorContains(t, err, "APP_ENV", env)
	}
}

func TestLoad_AcceptsEveryPlatformAppEnv(t *testing.T) {
	for _, env := range []string{"local", "dev", "test", "staging", "prod"} {
		t.Setenv("APP_ENV", env)
		t.Setenv("WORKFLOW_STREAM_GATEWAY_TOKEN", "secret")
		_, err := Load()
		assert.NoError(t, err, env)
	}
}

func TestLoad_StagingRequiresInternalToken(t *testing.T) {
	t.Setenv("APP_ENV", "staging")
	t.Setenv("WORKFLOW_STREAM_GATEWAY_TOKEN", "")

	_, err := Load()
	assert.ErrorContains(t, err, "WORKFLOW_STREAM_GATEWAY_TOKEN")
}

func TestLoad_LocalAndDevMayOmitTheToken(t *testing.T) {
	t.Setenv("WORKFLOW_STREAM_GATEWAY_TOKEN", "")
	for _, env := range []string{"local", "dev"} {
		t.Setenv("APP_ENV", env)
		_, err := Load()
		assert.NoError(t, err, env)
	}
}

func TestLoad_DefaultServiceName(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "")
	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "workflow-stream-gateway", cfg.OTELServiceName)
}

func TestLoad_ProdRequiresInternalToken(t *testing.T) {
	t.Setenv("APP_ENV", "prod")
	t.Setenv("WORKFLOW_STREAM_GATEWAY_TOKEN", "")

	_, err := Load()
	assert.ErrorContains(t, err, "WORKFLOW_STREAM_GATEWAY_TOKEN")
}

func TestLoad_ProdWithTokenSucceeds(t *testing.T) {
	t.Setenv("APP_ENV", "prod")
	t.Setenv("WORKFLOW_STREAM_GATEWAY_TOKEN", "secret")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "secret", cfg.InternalAPIToken)
}

func TestValidate_RejectsInvalidHTTPPort(t *testing.T) {
	cfg := validConfig()
	cfg.HTTPPort = 0
	assert.ErrorContains(t, cfg.validate(), "HTTP_PORT")
}

func TestValidate_RejectsInvalidMetricsPort(t *testing.T) {
	cfg := validConfig()
	cfg.MetricsPort = 0
	assert.ErrorContains(t, cfg.validate(), "METRICS_PORT")
}

func TestValidate_RejectsNonPositiveTicketTTL(t *testing.T) {
	cfg := validConfig()
	cfg.TicketTTL = 0
	assert.ErrorContains(t, cfg.validate(), "TICKET_TTL")
}

func TestValidate_RejectsNonPositiveRateMax(t *testing.T) {
	cfg := validConfig()
	cfg.TicketMintRateMax = 0
	assert.ErrorContains(t, cfg.validate(), "TICKET_MINT_RATE_MAX")
}

func TestValidate_RejectsNonPositiveRateWindow(t *testing.T) {
	cfg := validConfig()
	cfg.TicketMintRateWindow = 0
	assert.ErrorContains(t, cfg.validate(), "TICKET_MINT_RATE_WINDOW")
}

func TestValidate_RejectsNonPositiveRetentionWindow(t *testing.T) {
	cfg := validConfig()
	cfg.StreamRetentionWindow = 0
	assert.ErrorContains(t, cfg.validate(), "STREAM_RETENTION_WINDOW")
}

func TestValidate_RejectsKeyTTLNotExceedingRetentionWindow(t *testing.T) {
	cfg := validConfig()
	cfg.StreamKeyTTL = cfg.StreamRetentionWindow
	assert.ErrorContains(t, cfg.validate(), "STREAM_KEY_TTL")
}

func TestValidate_RejectsNonPositiveConnectionCap(t *testing.T) {
	cfg := validConfig()
	cfg.ConnectionCapPerUser = 0
	assert.ErrorContains(t, cfg.validate(), "CONNECTION_CAP_PER_USER")
}

func TestValidate_RejectsNonPositiveHeartbeat(t *testing.T) {
	cfg := validConfig()
	cfg.HeartbeatInterval = 0
	assert.ErrorContains(t, cfg.validate(), "HEARTBEAT_INTERVAL")
}

func validConfig() *Config {
	return &Config{
		AppEnv:                "dev",
		HTTPPort:              8080,
		MetricsPort:           9090,
		TicketTTL:             60 * time.Second,
		TicketMintRateMax:     10,
		TicketMintRateWindow:  60 * time.Second,
		StreamRetentionWindow: 10 * time.Minute,
		StreamKeyTTL:          15 * time.Minute,
		ConnectionCapPerUser:  5,
		HeartbeatInterval:     15 * time.Second,
	}
}
