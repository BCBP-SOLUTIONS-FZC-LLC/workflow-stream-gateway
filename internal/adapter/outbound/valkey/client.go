// Package valkey dials the service's only state store (LLD §2.3, §4):
// tickets, per-user event streams, and connection/rate counters.
package valkey

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Options mirrors redis.Options' fields this service actually configures —
// kept local rather than taking *config.Config directly, so this adapter
// package has no dependency on internal/config (the composition root in
// cmd/server reads config and passes plain values, matching this
// platform's existing valkey-adapter convention elsewhere).
type Options struct {
	Addr         string
	Password     string
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// NewClient dials Valkey and confirms reachability before returning — every
// caller of this service's endpoints depends on Valkey being up (LLD §9.4's
// fail-closed posture), so a dead connection is a boot failure, not
// something discovered on the first request.
func NewClient(ctx context.Context, opts Options) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         opts.Addr,
		Password:     opts.Password,
		DialTimeout:  opts.DialTimeout,
		ReadTimeout:  opts.ReadTimeout,
		WriteTimeout: opts.WriteTimeout,
	})
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("valkey ping: %w", err)
	}
	return client, nil
}
