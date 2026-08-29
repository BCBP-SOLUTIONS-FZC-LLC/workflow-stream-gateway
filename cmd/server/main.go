// Command server is the workflow-stream-gateway's only binary — an
// ordinary HTTP server with no background consumer goroutine and no
// Temporal dependency (LLD §2.2): every inbound path is a plain HTTP
// handler, since event ingestion arrives via event_consumer's own
// POST /internal/events convention rather than a dedicated SQS consumer.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/outbound/valkey"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability"
)

const shutdownTimeout = 10 * time.Second

// version is set via -ldflags at build time (Makefile/Dockerfile).
var version = "dev"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

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
			slog.Error("valkey close", "error", closeErr)
		}
	}()

	observability.Register()

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler: newRouter(cfg, client),
	}

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	// grants no API access, so NetworkPolicy can scope its own ingress
	metricsSrv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.MetricsPort),
		Handler: metricsMux,
	}

	errCh := make(chan error, 2)
	go func() {
		slog.Info("listening", "port", cfg.HTTPPort, "version", version)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()
	go func() {
		slog.Info("metrics listening", "port", cfg.MetricsPort)
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
		slog.Info("shutting down")
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
