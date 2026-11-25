//go:build integration

package valkey_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/outbound/valkey"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

// appendingSink writes a further event while the first frame is being sent,
// so that event exists only between two live reads, and ends the stream once
// the second frame arrives.
type appendingSink struct {
	appendNext func()
	cancel     context.CancelFunc
	types      []string
}

func (s *appendingSink) WriteFrame(_, eventType string, _ []byte) error {
	s.types = append(s.types, eventType)
	if len(s.types) == 1 {
		s.appendNext()
	}
	if len(s.types) == 2 {
		s.cancel()
	}
	return nil
}

func (s *appendingSink) WriteControlFrame(string, []byte) error { return nil }

func (s *appendingSink) WriteHeartbeat() error { return nil }

// countXReads counts the XREAD commands a client has sent.
type countXReads struct{ n atomic.Int32 }

func (h *countXReads) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *countXReads) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if strings.EqualFold(cmd.Name(), "xread") {
			h.n.Add(1)
		}
		return next(ctx, cmd)
	}
}

func (h *countXReads) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestStreamService_AnEventWrittenBetweenTwoLiveReadsIsDelivered(t *testing.T) {
	client := newClient(t)
	reader := redis.NewClient(&redis.Options{Addr: client.Options().Addr, ClientName: "live-tail"})
	t.Cleanup(func() { _ = reader.Close() })
	reads := &countXReads{}
	reader.AddHook(reads)
	events := valkey.NewEventStore(client, 10*time.Minute, 15*time.Minute)
	service := usecase.NewStreamService(nil, valkey.NewStreamStore(reader), valkey.NewConnectionLimiter(client), 3, time.Minute, 200*time.Millisecond)
	ticket := domain.Ticket{TenantID: "t1", UserID: "u1"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := &appendingSink{cancel: cancel, appendNext: func() {
		require.NoError(t, events.AppendEvent(context.Background(), "t1", "u1", "WorkflowTaskCompleted", []byte(`{"n":2}`)))
	}}
	served := make(chan error, 1)
	go func() { served <- service.Serve(ctx, ticket, "", sink) }()

	require.Eventually(t, func() bool { return blockedInXRead(client, "live-tail") }, 10*time.Second, 5*time.Millisecond, "the stream waits in its first live read")
	require.NoError(t, events.AppendEvent(context.Background(), "t1", "u1", "WorkflowTaskCreated", []byte(`{"n":1}`)))

	var serveErr error
	ended := false
	require.Eventually(t, func() bool {
		select {
		case serveErr = <-served:
			ended = true
			return true
		default:
		}
		return reads.n.Load() >= 2 && blockedInXRead(client, "live-tail")
	}, 10*time.Second, 5*time.Millisecond, "the stream either ends on the second frame or waits in its second live read")
	if !ended {
		cancel()
		serveErr = <-served
	}

	require.NoError(t, serveErr)
	assert.Equal(t, []string{"WorkflowTaskCreated", "WorkflowTaskCompleted"}, sink.types, "the event written while the first was sent arrives next")
}

func blockedInXRead(client *redis.Client, name string) bool {
	clients, err := client.ClientList(context.Background()).Result()
	if err != nil {
		return false
	}
	for _, line := range strings.Split(clients, "\n") {
		if strings.Contains(line, "name="+name) && strings.Contains(line, "cmd=xread") && strings.Contains(line, "flags=b") {
			return true
		}
	}
	return false
}
