package valkey

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// ConnectionLimiter implements usecase.ConnectionLimiter — the
// sse:conncount:<tenant_id>:<user_id> counter (LLD §4, §9.3).
type ConnectionLimiter struct {
	client redis.Cmdable
}

func NewConnectionLimiter(client redis.Cmdable) *ConnectionLimiter {
	return &ConnectionLimiter{client: client}
}

// TryAcquire increments the counter, refreshes its TTL (LLD §4: "EXPIRE
// refreshed on increment"), and rolls the increment back if that pushed
// the caller over cap.
func (l *ConnectionLimiter) TryAcquire(ctx context.Context, tenantID, userID string, cap int, ttl time.Duration) (bool, error) {
	key := fmt.Sprintf("sse:conncount:%s:%s", tenantID, userID)

	count, err := l.client.Incr(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("valkey incr: %w", err)
	}
	if err := l.client.Expire(ctx, key, ttl).Err(); err != nil {
		return false, fmt.Errorf("valkey expire: %w", err)
	}

	if count > int64(cap) {
		if err := l.client.Decr(ctx, key).Err(); err != nil {
			return false, fmt.Errorf("valkey decr: %w", err)
		}
		return false, nil
	}
	return true, nil
}

func (l *ConnectionLimiter) Release(ctx context.Context, tenantID, userID string) error {
	key := fmt.Sprintf("sse:conncount:%s:%s", tenantID, userID)
	if err := l.client.Decr(ctx, key).Err(); err != nil {
		return fmt.Errorf("valkey decr: %w", err)
	}
	return nil
}
