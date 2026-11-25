package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/logger"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/outbound/routingfile"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/outbound/valkey"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability"
)

const minShutdownTimeout = 10 * time.Second

const shutdownHeadroom = 5 * time.Second

var version = "dev"

type Logger interface {
	Debug(msg string, fields map[string]interface{})
	Info(msg string, fields map[string]interface{})
	Warn(msg string, fields map[string]interface{})
	Error(msg string, fields map[string]interface{})
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := serve(ctx, os.Stderr)
	stop()
	os.Exit(code)
}

func serve(ctx context.Context, stderr io.Writer) int {
	cfg, err := config.Load()
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "fatal:", err)
		return 1
	}

	log, err := logger.NewLogger(cfg.AppEnv)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "fatal: init logger:", err)
		return 1
	}

	if err := run(ctx, cfg, log); err != nil {
		log.Error("fatal", map[string]interface{}{"error": err.Error()})
		return 1
	}
	return 0
}

func run(ctx context.Context, cfg *config.Config, log Logger) error {
	routes, err := routingfile.Load(cfg.RoutingConfigPath)
	if err != nil {
		return err
	}

	dialCtx, cancel := context.WithTimeout(ctx, cfg.ValkeyDialTimeout)
	client, err := valkey.NewClient(dialCtx, valkey.Options{
		Addr:         cfg.ValkeyAddr,
		Password:     cfg.ValkeyPassword,
		DialTimeout:  cfg.ValkeyDialTimeout,
		ReadTimeout:  cfg.ValkeyReadTimeout,
		WriteTimeout: cfg.ValkeyWriteTimeout,
	})
	cancel()
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			log.Error("valkey close", map[string]interface{}{"error": closeErr.Error()})
		}
	}()

	ginCfg := observability.GinConfig(cfg.OTELServiceName, cfg.BuildVersion, cfg.AppEnv, log)
	shutdownTelemetry, err := observability.InitTelemetry(ginCfg, log)
	if err != nil {
		return err
	}
	defer shutdownTelemetry()

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler:           newRouter(ctx, cfg, ginCfg, client, routes),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", gincommon.MetricsHandler())
	metricsSrv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.MetricsPort),
		Handler:           metricsMux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 2)
	go func() {
		log.Info("listening", map[string]interface{}{"port": cfg.HTTPPort, "version": version})
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()
	go func() {
		log.Info("metrics listening", map[string]interface{}{"port": cfg.MetricsPort})
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		log.Info("shutting down", nil)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout(cfg.HeartbeatInterval))
	defer shutdownCancel()
	var errs []error
	if err := srv.Shutdown(shutdownCtx); err != nil {
		errs = append(errs, fmt.Errorf("server shutdown: %w", err))
	}
	if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
		errs = append(errs, fmt.Errorf("metrics server shutdown: %w", err))
	}
	return errors.Join(errs...)
}

func shutdownTimeout(heartbeat time.Duration) time.Duration {
	return max(minShutdownTimeout, heartbeat+shutdownHeadroom)
}
