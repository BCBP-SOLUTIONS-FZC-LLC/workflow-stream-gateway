package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	AppEnv       string
	BuildVersion string

	HTTPPort int
	// MetricsPort is deliberately a separate port from HTTPPort — /metrics
	// grants no API access, and NetworkPolicy scopes each port's ingress
	// independently (per-service Helm chart, punch-list item 7).
	MetricsPort int

	ValkeyAddr         string
	ValkeyPassword     string
	ValkeyDialTimeout  time.Duration
	ValkeyReadTimeout  time.Duration
	ValkeyWriteTimeout time.Duration

	// InternalAPIToken guards POST /internal/events (LLD §3.2's
	// RequireInternalToken) — empty disables the check (dev only).
	InternalAPIToken string

	TicketTTL            time.Duration
	TicketMintRateMax    int
	TicketMintRateWindow time.Duration

	// StreamRetentionWindow is the XADD MINID trim window (LLD §4, §8);
	// StreamKeyTTL is the Valkey key EXPIRE, refreshed on every write and
	// kept longer than the retention window so a quiet stream still
	// answers a late XRANGE backfill within the window.
	StreamRetentionWindow time.Duration
	StreamKeyTTL          time.Duration

	ConnectionCapPerUser int

	HeartbeatInterval time.Duration
}

func Load() (*Config, error) {
	_ = godotenv.Load()

	cfg := &Config{
		AppEnv:       getEnvOrDefault("APP_ENV", "dev"),
		BuildVersion: getEnvOrDefault("BUILD_VERSION", "dev"),

		HTTPPort:    getEnvIntOrDefault("HTTP_PORT", 8080),
		MetricsPort: getEnvIntOrDefault("METRICS_PORT", 9090),

		ValkeyAddr:         getEnvOrDefault("VALKEY_ADDR", "localhost:6379"),
		ValkeyPassword:     getEnvOrDefault("VALKEY_PASSWORD", ""),
		ValkeyDialTimeout:  getEnvDurationOrDefault("VALKEY_DIAL_TIMEOUT", 2*time.Second),
		ValkeyReadTimeout:  getEnvDurationOrDefault("VALKEY_READ_TIMEOUT", 1*time.Second),
		ValkeyWriteTimeout: getEnvDurationOrDefault("VALKEY_WRITE_TIMEOUT", 1*time.Second),

		InternalAPIToken: getEnvOrDefault("WORKFLOW_STREAM_GATEWAY_TOKEN", ""),

		TicketTTL:            getEnvDurationOrDefault("TICKET_TTL", 60*time.Second),
		TicketMintRateMax:    getEnvIntOrDefault("TICKET_MINT_RATE_MAX", 10),
		TicketMintRateWindow: getEnvDurationOrDefault("TICKET_MINT_RATE_WINDOW", 60*time.Second),

		StreamRetentionWindow: getEnvDurationOrDefault("STREAM_RETENTION_WINDOW", 10*time.Minute),
		StreamKeyTTL:          getEnvDurationOrDefault("STREAM_KEY_TTL", 15*time.Minute),

		ConnectionCapPerUser: getEnvIntOrDefault("CONNECTION_CAP_PER_USER", 5),

		HeartbeatInterval: getEnvDurationOrDefault("HEARTBEAT_INTERVAL", 15*time.Second),
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) validate() error {
	if c.HTTPPort < 1 || c.HTTPPort > 65535 {
		return fmt.Errorf("HTTP_PORT must be in [1, 65535]")
	}
	if c.MetricsPort < 1 || c.MetricsPort > 65535 {
		return fmt.Errorf("METRICS_PORT must be in [1, 65535]")
	}
	if c.AppEnv == "prod" && c.InternalAPIToken == "" {
		return fmt.Errorf("WORKFLOW_STREAM_GATEWAY_TOKEN is required when APP_ENV=prod")
	}
	if c.TicketTTL <= 0 {
		return fmt.Errorf("TICKET_TTL must be > 0")
	}
	if c.TicketMintRateMax <= 0 {
		return fmt.Errorf("TICKET_MINT_RATE_MAX must be > 0")
	}
	if c.TicketMintRateWindow <= 0 {
		return fmt.Errorf("TICKET_MINT_RATE_WINDOW must be > 0")
	}
	if c.StreamRetentionWindow <= 0 {
		return fmt.Errorf("STREAM_RETENTION_WINDOW must be > 0")
	}
	if c.StreamKeyTTL <= c.StreamRetentionWindow {
		return fmt.Errorf("STREAM_KEY_TTL must be > STREAM_RETENTION_WINDOW")
	}
	if c.ConnectionCapPerUser <= 0 {
		return fmt.Errorf("CONNECTION_CAP_PER_USER must be > 0")
	}
	if c.HeartbeatInterval <= 0 {
		return fmt.Errorf("HEARTBEAT_INTERVAL must be > 0")
	}
	return nil
}

func getEnvOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvIntOrDefault(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return i
}

func getEnvDurationOrDefault(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}
