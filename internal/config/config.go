package config

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	AppEnv          string
	BuildVersion    string
	OTELServiceName string

	HTTPPort    int
	MetricsPort int

	ValkeyAddr         string
	ValkeyPassword     string
	ValkeyDialTimeout  time.Duration
	ValkeyReadTimeout  time.Duration
	ValkeyWriteTimeout time.Duration

	InternalAPIToken string

	TicketTTL            time.Duration
	TicketMintRateMax    int
	TicketMintRateWindow time.Duration

	StreamRetentionWindow time.Duration
	StreamKeyTTL          time.Duration

	ConnectionCapPerUser int

	HeartbeatInterval time.Duration
}

func Load() (*Config, error) {
	_ = godotenv.Load()

	var parseErr error
	record := func(err error) {
		if err != nil && parseErr == nil {
			parseErr = err
		}
	}
	envInt := func(key string, fallback int) int {
		v, err := getEnvInt(key, fallback)
		record(err)
		return v
	}
	envDuration := func(key string, fallback time.Duration) time.Duration {
		v, err := getEnvDuration(key, fallback)
		record(err)
		return v
	}

	cfg := &Config{
		AppEnv:          getEnvOrDefault("APP_ENV", "dev"),
		BuildVersion:    getEnvOrDefault("BUILD_VERSION", "dev"),
		OTELServiceName: getEnvOrDefault("OTEL_SERVICE_NAME", "workflow-stream-gateway"),

		HTTPPort:    envInt("HTTP_PORT", 8080),
		MetricsPort: envInt("METRICS_PORT", 9090),

		ValkeyAddr:         getEnvOrDefault("VALKEY_ADDR", "localhost:6379"),
		ValkeyPassword:     getEnvOrDefault("VALKEY_PASSWORD", ""),
		ValkeyDialTimeout:  envDuration("VALKEY_DIAL_TIMEOUT", 2*time.Second),
		ValkeyReadTimeout:  envDuration("VALKEY_READ_TIMEOUT", 1*time.Second),
		ValkeyWriteTimeout: envDuration("VALKEY_WRITE_TIMEOUT", 1*time.Second),

		InternalAPIToken: getEnvOrDefault("WORKFLOW_STREAM_GATEWAY_TOKEN", ""),

		TicketTTL:            envDuration("TICKET_TTL", 60*time.Second),
		TicketMintRateMax:    envInt("TICKET_MINT_RATE_MAX", 10),
		TicketMintRateWindow: envDuration("TICKET_MINT_RATE_WINDOW", 60*time.Second),

		StreamRetentionWindow: envDuration("STREAM_RETENTION_WINDOW", 10*time.Minute),
		StreamKeyTTL:          envDuration("STREAM_KEY_TTL", 15*time.Minute),

		ConnectionCapPerUser: envInt("CONNECTION_CAP_PER_USER", 5),

		HeartbeatInterval: envDuration("HEARTBEAT_INTERVAL", 15*time.Second),
	}

	if parseErr != nil {
		return nil, parseErr
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

var appEnvs = []string{"local", "dev", "test", "staging", "prod"}

func (c *Config) IsDev() bool {
	return c.AppEnv == "dev" || c.AppEnv == "local"
}

func (c *Config) validate() error {
	if !slices.Contains(appEnvs, c.AppEnv) {
		return fmt.Errorf("APP_ENV must be one of %s", strings.Join(appEnvs, ", "))
	}
	if c.HTTPPort < 1 || c.HTTPPort > 65535 {
		return fmt.Errorf("HTTP_PORT must be in [1, 65535]")
	}
	if c.MetricsPort < 1 || c.MetricsPort > 65535 {
		return fmt.Errorf("METRICS_PORT must be in [1, 65535]")
	}
	if !c.IsDev() && c.InternalAPIToken == "" {
		return fmt.Errorf("WORKFLOW_STREAM_GATEWAY_TOKEN is required outside dev and local")
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

func getEnvInt(key string, fallback int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	return i, nil
}

func getEnvDuration(key string, fallback time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration: %w", key, err)
	}
	return d, nil
}
