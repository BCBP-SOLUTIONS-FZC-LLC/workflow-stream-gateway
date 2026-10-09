package valkey

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Options struct {
	Addr         string
	Password     string
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

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
