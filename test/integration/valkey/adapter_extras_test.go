//go:build integration

package valkey_test

import (
	"context"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/outbound/valkey"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability/obstest"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

func TestEventStore_FramesWrittenCounterFollowsEventType(t *testing.T) {
	obstest.Init(t)
	client := newClient(t)
	store := valkey.NewEventStore(client, time.Hour, time.Hour)
	ctx := context.Background()

	cases := []struct {
		name      string
		collector bool
	}{
		{name: "collector present counts per event type", collector: true},
		{name: "nil collector is skipped without error", collector: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			saved := observability.FramesWrittenTotal
			t.Cleanup(func() { observability.FramesWrittenTotal = saved })

			if !tc.collector {
				observability.FramesWrittenTotal = nil
				require.NoError(t, store.AppendEvent(ctx, "t-metric", "u1", "MetricNilType", []byte(`{}`)))
				return
			}

			counter := observability.FramesWrittenTotal.WithLabelValues("MetricTypeA")
			other := observability.FramesWrittenTotal.WithLabelValues("MetricTypeB")
			beforeA, beforeB := counterValueForValkeyExtras(counter), counterValueForValkeyExtras(other)

			require.NoError(t, store.AppendEvent(ctx, "t-metric", "u1", "MetricTypeA", []byte(`{}`)))
			require.NoError(t, store.AppendBroadcastEvent(ctx, "t-metric", "MetricTypeA", []byte(`{}`)))

			assert.Equal(t, beforeA+2, counterValueForValkeyExtras(counter))
			assert.Equal(t, beforeB, counterValueForValkeyExtras(other), "other event types are untouched")
		})
	}
}

func counterValueForValkeyExtras(c interface{ Write(*dto.Metric) error }) float64 {
	m := &dto.Metric{}
	if err := c.Write(m); err != nil {
		panic(err)
	}
	return m.GetCounter().GetValue()
}

func TestStreamStore_EntriesWithMissingFieldsDecodeToEmptyValues(t *testing.T) {
	client := newClient(t)
	store := valkey.NewStreamStore(client)
	ctx := context.Background()
	target := usecase.StreamTarget{TenantID: "t-decode", UserID: "u1"}
	key := store.KeyFor(target)

	cases := []struct {
		name      string
		values    map[string]any
		wantEvent string
		wantData  string
		wantNil   bool
	}{
		{name: "event field missing", values: map[string]any{"data": "payload"}, wantEvent: "", wantData: "payload"},
		{name: "data field missing", values: map[string]any{"event": "Evt"}, wantEvent: "Evt", wantNil: true},
		{name: "data stored as bytes", values: map[string]any{"event": "Evt", "data": []byte("raw")}, wantEvent: "Evt", wantData: "raw"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, client.Del(ctx, key).Err())
			require.NoError(t, client.XAdd(ctx, &redis.XAddArgs{Stream: key, Values: tc.values}).Err())

			frames, err := store.RangeSince(ctx, target, "0-0")
			require.NoError(t, err)
			require.Len(t, frames, 1)
			assert.Equal(t, tc.wantEvent, frames[0].EventType)
			assert.Equal(t, key, frames[0].SourceKey)
			if tc.wantNil {
				assert.Nil(t, frames[0].Data)
			} else {
				assert.Equal(t, tc.wantData, string(frames[0].Data))
			}
		})
	}
}

func TestNewClient_ReachableValkeyReturnsAUsableClient(t *testing.T) {
	addr := newClient(t).Options().Addr

	client, err := valkey.NewClient(context.Background(), valkey.Options{Addr: addr, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	require.NotNil(t, client)
	t.Cleanup(func() { _ = client.Close() })

	assert.NoError(t, client.Ping(context.Background()).Err())
}
