// Command server is the workflow-stream-gateway's only binary — an
// ordinary HTTP server with no background consumer goroutine and no
// Temporal dependency (LLD §2.2): every inbound path is a plain HTTP
// handler, since event ingestion arrives via event_consumer's own
// POST /api/v1/internal/events convention rather than a dedicated SQS consumer.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/logger"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/outbound/valkey"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability"
)

const shutdownTimeout = 10 * time.Second

// version is set via -ldflags at build time (Makefile/Dockerfile).
var version = "dev"

// Logger is the interface for the logger used by the application.
// It is named locally since this service has no internal/core/port package of its own
type Logger interface {
	Debug(msg string, fields map[string]interface{})
	Info(msg string, fields map[string]interface{})
	Warn(msg string, fields map[string]interface{})
	Error(msg string, fields map[string]interface{})
}

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}

	log, err := logger.NewLogger(cfg.AppEnv)
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal: init logger:", err)
		os.Exit(1)
	}

	if err := run(cfg, log); err != nil {
		log.Error("fatal", map[string]interface{}{"error": err.Error()})
		os.Exit(1)
	}
}

func run(cfg *config.Config, log Logger) error {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.ValkeyDialTimeout)
	client, err := valkey.NewClient(ctx, valkey.Options{
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

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler: newRouter(cfg, client, log),
	}

	// Register() must run after newRouter (which calls
	// gincommon.ObservabilityMiddlewares, wiring in this process's real
	// gincommon.MetricsRegisterer()) — registering this package's own
	// collectors any earlier would fall back to gincommon's default
	// pre-init registerer instead of the one gincommon.MetricsRegisterer's
	// own doc comment promises callers.
	observability.Register()

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	// grants no API access, so NetworkPolicy can scope its own ingress
	metricsSrv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.MetricsPort),
		Handler: metricsMux,
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

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return err
	case <-stop:
		log.Info("shutting down", nil)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("server shutdown: %w", err)
	}
	if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("metrics server shutdown: %w", err)
	}
	return nil
}
