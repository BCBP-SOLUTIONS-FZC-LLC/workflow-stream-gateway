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

var acquireScript = redis.NewScript(`
local count = tonumber(redis.call('GET', KEYS[1]) or '0')
if count >= tonumber(ARGV[1]) then
  return 0
end
redis.call('INCR', KEYS[1])
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1
`)

func (l *ConnectionLimiter) TryAcquire(ctx context.Context, tenantID, userID string, cap int, ttl time.Duration) (bool, error) {
	key := fmt.Sprintf("sse:conncount:%s:%s", tenantID, userID)
	granted, err := acquireScript.Run(ctx, l.client, []string{key}, cap, ttl.Milliseconds()).Int64()
	if err != nil {
		return false, fmt.Errorf("valkey acquire: %w", err)
	}
	return granted == 1, nil
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
