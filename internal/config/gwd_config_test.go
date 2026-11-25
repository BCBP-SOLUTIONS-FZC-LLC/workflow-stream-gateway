package config

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var gwDEnvKeys = []string{
	"APP_ENV", "BUILD_VERSION", "OTEL_SERVICE_NAME",
	"HTTP_PORT", "METRICS_PORT", "ROUTING_CONFIG_PATH", "CALLER_IDENTITY_CHECK",
	"VALKEY_ADDR", "VALKEY_PASSWORD", "VALKEY_DIAL_TIMEOUT", "VALKEY_READ_TIMEOUT", "VALKEY_WRITE_TIMEOUT",
	"TICKET_TTL", "TICKET_MINT_RATE_MAX", "TICKET_MINT_RATE_WINDOW",
	"STREAM_RETENTION_WINDOW", "STREAM_KEY_TTL", "CONNECTION_CAP_PER_USER", "HEARTBEAT_INTERVAL",
}

// gwDEmptyEnv sets every key Load reads to the empty string, so the test does not depend on the
// machine's environment. An empty value counts as unset.
func gwDEmptyEnv(t *testing.T) {
	t.Helper()
	for _, key := range gwDEnvKeys {
		t.Setenv(key, "")
	}
}

func gwDDefaultConfig() Config {
	return Config{
		AppEnv:                "dev",
		BuildVersion:          "dev",
		OTELServiceName:       "workflow-stream-gateway",
		HTTPPort:              8080,
		MetricsPort:           9090,
		ValkeyAddr:            "localhost:6379",
		ValkeyPassword:        "",
		ValkeyDialTimeout:     2 * time.Second,
		ValkeyReadTimeout:     time.Second,
		ValkeyWriteTimeout:    time.Second,
		TicketTTL:             60 * time.Second,
		TicketMintRateMax:     10,
		TicketMintRateWindow:  60 * time.Second,
		StreamRetentionWindow: 10 * time.Minute,
		StreamKeyTTL:          15 * time.Minute,
		ConnectionCapPerUser:  5,
		HeartbeatInterval:     15 * time.Second,
		RoutingConfigPath:     "config/routing.yaml",
		CallerIdentityCheck:   true,
	}
}

func TestGwD_Load_AnIntegerKeyThatIsNotAnIntegerFailsNamingTheKey(t *testing.T) {
	keys := []string{"HTTP_PORT", "METRICS_PORT", "TICKET_MINT_RATE_MAX", "CONNECTION_CAP_PER_USER"}
	values := []string{"abc", " 8080", "8080 ", "8080.0", "0x1F", "1e3", "9999999999999999999999"}
	for _, key := range keys {
		for _, value := range values {
			t.Run(key+"="+value, func(t *testing.T) {
				gwDEmptyEnv(t)
				t.Setenv(key, value)

				cfg, err := Load()

				require.Error(t, err)
				assert.Nil(t, cfg)
				assert.ErrorContains(t, err, key+" must be an integer: ")
				var numErr *strconv.NumError
				assert.True(t, errors.As(err, &numErr), "the parse error stays reachable")
			})
		}
	}
}

func TestGwD_Load_TheIntegerErrorNamesTheValueThatFailed(t *testing.T) {
	gwDEmptyEnv(t)
	t.Setenv("HTTP_PORT", "abc")

	_, err := Load()

	assert.EqualError(t, err, `HTTP_PORT must be an integer: strconv.Atoi: parsing "abc": invalid syntax`)
}

func TestGwD_Load_BooleanForms(t *testing.T) {
	cases := []struct {
		value string
		want  bool
	}{
		{"1", true}, {"t", true}, {"T", true}, {"true", true}, {"TRUE", true}, {"True", true},
		{"0", false}, {"f", false}, {"F", false}, {"false", false}, {"FALSE", false}, {"False", false},
	}
	for _, tc := range cases {
		t.Run(tc.value, func(t *testing.T) {
			gwDEmptyEnv(t)
			t.Setenv("APP_ENV", "dev")
			t.Setenv("CALLER_IDENTITY_CHECK", tc.value)

			cfg, err := Load()

			require.NoError(t, err)
			assert.Equal(t, tc.want, cfg.CallerIdentityCheck)
		})
	}
}

func TestGwD_Load_ABooleanThatIsNeitherFormFailsWithTheValueQuoted(t *testing.T) {
	for _, value := range []string{"maybe", "yes", "no", "on", "off", "tru", "tRuE", " true", "true ", "2"} {
		t.Run(value, func(t *testing.T) {
			gwDEmptyEnv(t)
			t.Setenv("CALLER_IDENTITY_CHECK", value)

			cfg, err := Load()

			assert.Nil(t, cfg)
			assert.EqualError(t, err, "CALLER_IDENTITY_CHECK must be true or false, got "+strconv.Quote(value))
		})
	}
}

func TestGwD_Load_DurationForms(t *testing.T) {
	cases := []struct {
		value string
		want  time.Duration
	}{
		{"30s", 30 * time.Second},
		{"1m", time.Minute},
		{"1h30m", 90 * time.Minute},
		{"250ms", 250 * time.Millisecond},
		{"1m30s", 90 * time.Second},
		{"1.5s", 1500 * time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.value, func(t *testing.T) {
			gwDEmptyEnv(t)
			t.Setenv("TICKET_TTL", tc.value)

			cfg, err := Load()

			require.NoError(t, err)
			assert.Equal(t, tc.want, cfg.TicketTTL)
		})
	}
}

func TestGwD_Load_ADurationNeedsAUnit(t *testing.T) {
	keys := []string{
		"VALKEY_DIAL_TIMEOUT", "VALKEY_READ_TIMEOUT", "VALKEY_WRITE_TIMEOUT",
		"TICKET_TTL", "TICKET_MINT_RATE_WINDOW", "STREAM_RETENTION_WINDOW", "STREAM_KEY_TTL", "HEARTBEAT_INTERVAL",
	}
	values := []string{"60", "soon", "1 s", "5sec", "s", "-"}
	for _, key := range keys {
		for _, value := range values {
			t.Run(key+"="+value, func(t *testing.T) {
				gwDEmptyEnv(t)
				t.Setenv(key, value)

				cfg, err := Load()

				require.Error(t, err)
				assert.Nil(t, cfg)
				assert.ErrorContains(t, err, key+" must be a duration: ")
			})
		}
	}
}

func TestGwD_Load_TheDurationErrorNamesTheValueThatFailed(t *testing.T) {
	gwDEmptyEnv(t)
	t.Setenv("TICKET_TTL", "60")

	_, err := Load()

	assert.EqualError(t, err, `TICKET_TTL must be a duration: time: missing unit in duration "60"`)
}

func TestGwD_Validate_DurationsMustBePositive(t *testing.T) {
	setters := []struct {
		key string
		set func(*Config, time.Duration)
	}{
		{"TICKET_TTL", func(c *Config, d time.Duration) { c.TicketTTL = d }},
		{"TICKET_MINT_RATE_WINDOW", func(c *Config, d time.Duration) { c.TicketMintRateWindow = d }},
		{"STREAM_RETENTION_WINDOW", func(c *Config, d time.Duration) { c.StreamRetentionWindow = d }},
		{"HEARTBEAT_INTERVAL", func(c *Config, d time.Duration) { c.HeartbeatInterval = d }},
	}
	values := []struct {
		name  string
		value time.Duration
		valid bool
	}{
		{"zero", 0, false},
		{"minus one second", -time.Second, false},
		{"minus one nanosecond", -time.Nanosecond, false},
		{"one nanosecond", time.Nanosecond, true},
	}
	for _, s := range setters {
		for _, v := range values {
			t.Run(s.key+" "+v.name, func(t *testing.T) {
				cfg := validConfig()
				s.set(cfg, v.value)

				err := cfg.validate()

				if v.valid {
					assert.NoError(t, err)
					return
				}
				assert.EqualError(t, err, s.key+" must be > 0")
			})
		}
	}
}

func TestGwD_Load_ANegativeDurationFromTheEnvironmentIsRefused(t *testing.T) {
	for _, key := range []string{"TICKET_TTL", "TICKET_MINT_RATE_WINDOW", "STREAM_RETENTION_WINDOW", "HEARTBEAT_INTERVAL"} {
		for _, value := range []string{"0s", "-1s"} {
			t.Run(key+"="+value, func(t *testing.T) {
				gwDEmptyEnv(t)
				t.Setenv(key, value)

				cfg, err := Load()

				assert.Nil(t, cfg)
				assert.EqualError(t, err, key+" must be > 0")
			})
		}
	}
}

func TestGwD_Validate_CountsMustBePositive(t *testing.T) {
	setters := []struct {
		key string
		set func(*Config, int)
	}{
		{"TICKET_MINT_RATE_MAX", func(c *Config, n int) { c.TicketMintRateMax = n }},
		{"CONNECTION_CAP_PER_USER", func(c *Config, n int) { c.ConnectionCapPerUser = n }},
	}
	values := []struct {
		name  string
		value int
		valid bool
	}{
		{"zero", 0, false},
		{"minus one", -1, false},
		{"one", 1, true},
	}
	for _, s := range setters {
		for _, v := range values {
			t.Run(s.key+" "+v.name, func(t *testing.T) {
				cfg := validConfig()
				s.set(cfg, v.value)

				err := cfg.validate()

				if v.valid {
					assert.NoError(t, err)
					return
				}
				assert.EqualError(t, err, s.key+" must be > 0")
			})
		}
	}
}

func TestGwD_Load_ACountOfZeroOrBelowFromTheEnvironmentIsRefused(t *testing.T) {
	for _, key := range []string{"TICKET_MINT_RATE_MAX", "CONNECTION_CAP_PER_USER"} {
		for _, value := range []string{"0", "-1"} {
			t.Run(key+"="+value, func(t *testing.T) {
				gwDEmptyEnv(t)
				t.Setenv(key, value)

				cfg, err := Load()

				assert.Nil(t, cfg)
				assert.EqualError(t, err, key+" must be > 0")
			})
		}
	}
}

func TestGwD_Validate_KeyTTLMustExceedTheRetentionWindow(t *testing.T) {
	cases := []struct {
		name      string
		keyTTL    time.Duration
		retention time.Duration
		valid     bool
	}{
		{"equal", 10 * time.Minute, 10 * time.Minute, false},
		{"one nanosecond below", 10*time.Minute - time.Nanosecond, 10 * time.Minute, false},
		{"far below", time.Minute, 10 * time.Minute, false},
		{"one nanosecond above", 10*time.Minute + time.Nanosecond, 10 * time.Minute, true},
		{"minimal pair", 2 * time.Nanosecond, time.Nanosecond, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.StreamRetentionWindow = tc.retention
			cfg.StreamKeyTTL = tc.keyTTL

			err := cfg.validate()

			if tc.valid {
				assert.NoError(t, err)
				return
			}
			assert.EqualError(t, err, "STREAM_KEY_TTL must be > STREAM_RETENTION_WINDOW")
		})
	}
}

func TestGwD_Load_KeyTTLEqualToTheRetentionWindowFromTheEnvironmentIsRefused(t *testing.T) {
	cases := []struct {
		name   string
		keyTTL string
		valid  bool
	}{
		{"equal", "5m", false},
		{"one nanosecond above", "5m0.000000001s", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gwDEmptyEnv(t)
			t.Setenv("STREAM_RETENTION_WINDOW", "5m")
			t.Setenv("STREAM_KEY_TTL", tc.keyTTL)

			cfg, err := Load()

			if tc.valid {
				require.NoError(t, err)
				assert.Equal(t, 5*time.Minute+time.Nanosecond, cfg.StreamKeyTTL)
				return
			}
			assert.Nil(t, cfg)
			assert.EqualError(t, err, "STREAM_KEY_TTL must be > STREAM_RETENTION_WINDOW")
		})
	}
}

func TestGwD_Load_EveryKeySetToTheEmptyStringTakesItsDefault(t *testing.T) {
	gwDEmptyEnv(t)

	cfg, err := Load()

	require.NoError(t, err)
	assert.Equal(t, gwDDefaultConfig(), *cfg)
}

func TestGwD_Load_EachKeyEmptyWhileOthersAreSetTakesItsOwnDefault(t *testing.T) {
	// One key at a time is emptied while every other key holds a non-default value, so a getter
	// that read the wrong key or kept the empty string would show.
	overrides := gwDNonDefaultEnv()
	want := gwDNonDefaultConfig()
	defaults := gwDDefaultConfig()
	fieldOf := map[string]func(c *Config, d Config){
		"APP_ENV":                 func(c *Config, d Config) { c.AppEnv = d.AppEnv },
		"BUILD_VERSION":           func(c *Config, d Config) { c.BuildVersion = d.BuildVersion },
		"OTEL_SERVICE_NAME":       func(c *Config, d Config) { c.OTELServiceName = d.OTELServiceName },
		"HTTP_PORT":               func(c *Config, d Config) { c.HTTPPort = d.HTTPPort },
		"METRICS_PORT":            func(c *Config, d Config) { c.MetricsPort = d.MetricsPort },
		"ROUTING_CONFIG_PATH":     func(c *Config, d Config) { c.RoutingConfigPath = d.RoutingConfigPath },
		"CALLER_IDENTITY_CHECK":   func(c *Config, d Config) { c.CallerIdentityCheck = d.CallerIdentityCheck },
		"VALKEY_ADDR":             func(c *Config, d Config) { c.ValkeyAddr = d.ValkeyAddr },
		"VALKEY_PASSWORD":         func(c *Config, d Config) { c.ValkeyPassword = d.ValkeyPassword },
		"VALKEY_DIAL_TIMEOUT":     func(c *Config, d Config) { c.ValkeyDialTimeout = d.ValkeyDialTimeout },
		"VALKEY_READ_TIMEOUT":     func(c *Config, d Config) { c.ValkeyReadTimeout = d.ValkeyReadTimeout },
		"VALKEY_WRITE_TIMEOUT":    func(c *Config, d Config) { c.ValkeyWriteTimeout = d.ValkeyWriteTimeout },
		"TICKET_TTL":              func(c *Config, d Config) { c.TicketTTL = d.TicketTTL },
		"TICKET_MINT_RATE_MAX":    func(c *Config, d Config) { c.TicketMintRateMax = d.TicketMintRateMax },
		"TICKET_MINT_RATE_WINDOW": func(c *Config, d Config) { c.TicketMintRateWindow = d.TicketMintRateWindow },
		"CONNECTION_CAP_PER_USER": func(c *Config, d Config) { c.ConnectionCapPerUser = d.ConnectionCapPerUser },
		"HEARTBEAT_INTERVAL":      func(c *Config, d Config) { c.HeartbeatInterval = d.HeartbeatInterval },
		// The two window keys are checked against each other, so each is emptied together with
		// the other's default in the dedicated test below.
	}
	require.Equal(t, len(gwDEnvKeys)-2, len(fieldOf), "every key except the two linked windows has a case")

	for key, reset := range fieldOf {
		t.Run(key, func(t *testing.T) {
			for k, v := range overrides {
				t.Setenv(k, v)
			}
			t.Setenv(key, "")
			expected := want
			reset(&expected, defaults)

			cfg, err := Load()

			require.NoError(t, err)
			assert.Equal(t, expected, *cfg)
		})
	}
}

func TestGwD_Load_TheLinkedWindowKeysTakeTheirDefaultsWhenEmpty(t *testing.T) {
	cases := []struct {
		key  string
		with map[string]string
		got  func(*Config) time.Duration
		want time.Duration
	}{
		{"STREAM_RETENTION_WINDOW", map[string]string{"STREAM_KEY_TTL": "20m"}, func(c *Config) time.Duration { return c.StreamRetentionWindow }, 10 * time.Minute},
		{"STREAM_KEY_TTL", map[string]string{"STREAM_RETENTION_WINDOW": "1m"}, func(c *Config) time.Duration { return c.StreamKeyTTL }, 15 * time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			gwDEmptyEnv(t)
			for k, v := range tc.with {
				t.Setenv(k, v)
			}
			t.Setenv(tc.key, "")

			cfg, err := Load()

			require.NoError(t, err)
			assert.Equal(t, tc.want, tc.got(cfg))
		})
	}
}

func TestGwD_Load_EveryKeySetToANonDefaultValueIsParsedIntoItsField(t *testing.T) {
	for k, v := range gwDNonDefaultEnv() {
		t.Setenv(k, v)
	}

	cfg, err := Load()

	require.NoError(t, err)
	assert.Equal(t, gwDNonDefaultConfig(), *cfg)
}

func gwDNonDefaultEnv() map[string]string {
	return map[string]string{
		"APP_ENV":                 "test",
		"BUILD_VERSION":           "v9.8.7",
		"OTEL_SERVICE_NAME":       "gw-under-test",
		"HTTP_PORT":               "18080",
		"METRICS_PORT":            "19090",
		"ROUTING_CONFIG_PATH":     "/etc/gw/routes.yaml",
		"CALLER_IDENTITY_CHECK":   "false",
		"VALKEY_ADDR":             "valkey.internal:6380",
		"VALKEY_PASSWORD":         "s3cret-for-test",
		"VALKEY_DIAL_TIMEOUT":     "3s",
		"VALKEY_READ_TIMEOUT":     "400ms",
		"VALKEY_WRITE_TIMEOUT":    "700ms",
		"TICKET_TTL":              "45s",
		"TICKET_MINT_RATE_MAX":    "7",
		"TICKET_MINT_RATE_WINDOW": "2m",
		"STREAM_RETENTION_WINDOW": "5m",
		"STREAM_KEY_TTL":          "8m",
		"CONNECTION_CAP_PER_USER": "3",
		"HEARTBEAT_INTERVAL":      "20s",
	}
}

func gwDNonDefaultConfig() Config {
	return Config{
		AppEnv:                "test",
		BuildVersion:          "v9.8.7",
		OTELServiceName:       "gw-under-test",
		HTTPPort:              18080,
		MetricsPort:           19090,
		ValkeyAddr:            "valkey.internal:6380",
		ValkeyPassword:        "s3cret-for-test",
		ValkeyDialTimeout:     3 * time.Second,
		ValkeyReadTimeout:     400 * time.Millisecond,
		ValkeyWriteTimeout:    700 * time.Millisecond,
		TicketTTL:             45 * time.Second,
		TicketMintRateMax:     7,
		TicketMintRateWindow:  2 * time.Minute,
		StreamRetentionWindow: 5 * time.Minute,
		StreamKeyTTL:          8 * time.Minute,
		ConnectionCapPerUser:  3,
		HeartbeatInterval:     20 * time.Second,
		RoutingConfigPath:     "/etc/gw/routes.yaml",
		CallerIdentityCheck:   false,
	}
}
