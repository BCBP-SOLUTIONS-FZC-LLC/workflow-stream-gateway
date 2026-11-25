package valkey

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestNewStreamClient_KeepsTheConnectionSettingsWithItsOwnPool(t *testing.T) {
	base := redis.NewClient(&redis.Options{Addr: "valkey:6379", Password: "pw", DialTimeout: 2 * time.Second, ReadTimeout: time.Second, PoolSize: 20})
	t.Cleanup(func() { _ = base.Close() })

	stream := NewStreamClient(base, 1000)
	t.Cleanup(func() { _ = stream.Close() })

	assert.Equal(t, 1000, stream.Options().PoolSize)
	assert.Equal(t, 20, base.Options().PoolSize, "the base client keeps its own pool")
	assert.Equal(t, "valkey:6379", stream.Options().Addr)
	assert.Equal(t, "pw", stream.Options().Password)
	assert.Equal(t, 2*time.Second, stream.Options().DialTimeout)
	assert.Equal(t, time.Second, stream.Options().ReadTimeout)
}
