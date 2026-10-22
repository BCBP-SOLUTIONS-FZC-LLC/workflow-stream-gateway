package valkey

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type ConnectionLimiter struct {
	client redis.Cmdable
}

func NewConnectionLimiter(client redis.Cmdable) *ConnectionLimiter {
	return &ConnectionLimiter{client: client}
}

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

var releaseScript = redis.NewScript(`
local count = redis.call('DECR', KEYS[1])
if count <= 0 then
  redis.call('DEL', KEYS[1])
end
return count
`)

func (l *ConnectionLimiter) Release(ctx context.Context, tenantID, userID string) error {
	key := fmt.Sprintf("sse:conncount:%s:%s", tenantID, userID)
	if err := releaseScript.Run(ctx, l.client, []string{key}).Err(); err != nil {
		return fmt.Errorf("valkey release: %w", err)
	}
	return nil
}

func (l *ConnectionLimiter) Refresh(ctx context.Context, tenantID, userID string, ttl time.Duration) error {
	key := fmt.Sprintf("sse:conncount:%s:%s", tenantID, userID)
	if err := l.client.Expire(ctx, key, ttl).Err(); err != nil {
		return fmt.Errorf("valkey expire: %w", err)
	}
	return nil
}
