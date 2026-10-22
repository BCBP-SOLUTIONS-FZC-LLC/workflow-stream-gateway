package observability

import (
	"fmt"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
)

type Logger interface {
	Debug(msg string, fields map[string]interface{})
	Info(msg string, fields map[string]interface{})
	Warn(msg string, fields map[string]interface{})
	Error(msg string, fields map[string]interface{})
}

func GinConfig(serviceName, buildVersion, environment string, log Logger) gincommon.Config {
	return gincommon.Config{
		Logger:       log,
		ServiceName:  serviceName,
		BuildVersion: buildVersion,
		Domain:       Domain,
		Environment:  environment,
	}
}

func InitTelemetry(ginCfg gincommon.Config, log Logger) (func(), error) {
	shutdown, err := gincommon.InitTracingWithConfig(gincommon.TracingConfig{
		ServiceName:  ginCfg.ServiceName,
		BuildVersion: ginCfg.BuildVersion,
		Domain:       ginCfg.Domain,
		Environment:  ginCfg.Environment,
		Logger:       ginCfg.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("tracing: %w", err)
	}

	_ = gincommon.ObservabilityMiddlewares(ginCfg)
	Register()

	warnings, err := events.InitMetrics(events.MetricsIdentityFromLabels(gincommon.MetricsConstLabels()), gincommon.MetricsRegisterer())
	if err != nil {
		shutdown()
		return nil, fmt.Errorf("platform-events metrics: %w", err)
	}
	for _, w := range warnings {
		log.Warn("platform-events metric not registered", map[string]interface{}{"metric": w.Metric, "error": w.Error()})
	}

	return shutdown, nil
}
