// Package valkey dials the service's only state store (LLD §2.3, §4):
// tickets, per-user event streams, and connection/rate counters.
package valkey

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/config"
)

// NewClient dials Valkey and confirms reachability before returning — every
// caller of this service's endpoints depends on Valkey being up (LLD §9.4's
// fail-closed posture), so a dead connection is a boot failure, not
// something discovered on the first request.
func NewClient(ctx context.Context, cfg *config.Config) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         cfg.ValkeyAddr,
		Password:     cfg.ValkeyPassword,
		DialTimeout:  cfg.ValkeyDialTimeout,
		ReadTimeout:  cfg.ValkeyReadTimeout,
		WriteTimeout: cfg.ValkeyWriteTimeout,
	})
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("valkey ping: %w", err)
	}
	return client, nil
}
