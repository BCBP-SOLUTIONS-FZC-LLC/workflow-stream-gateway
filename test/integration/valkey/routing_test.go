//go:build integration

package valkey_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/outbound/routingfile"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/adapter/outbound/valkey"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

func TestFanout_AnIngestedEventIsStoredUnderItsServicePrefixedName(t *testing.T) {
	client := newClient(t)
	routes, err := routingfile.Load("../../../config/routing.yaml")
	require.NoError(t, err)
	fanout := usecase.NewFanoutService(valkey.NewEventStore(client, 10*time.Minute, 15*time.Minute), routes, usecase.CallerCheckOff)
	streams := valkey.NewStreamStore(client)
	ctx := context.Background()

	name, err := fanout.Ingest(ctx, usecase.IngestInput{
		Source:      "workflow-execution-svc",
		EventType:   "WorkflowTaskCreated",
		TenantID:    "t-routing",
		Payload:     []byte(`{"recipients":[{"kind":"user","id":"00000000-0000-4000-8000-0000000001a1"},{"kind":"department","id":"00000000-0000-4000-8000-0000000001d1"}]}`),
		RawEnvelope: []byte(`{"type":"WorkflowTaskCreated"}`),
	})
	require.NoError(t, err)
	assert.Equal(t, "workflow.WorkflowTaskCreated", name)

	for _, target := range []usecase.StreamTarget{
		{TenantID: "t-routing", UserID: "00000000-0000-4000-8000-0000000001a1"},
		{TenantID: "t-routing"},
		{TenantID: "t-routing", DepartmentID: "00000000-0000-4000-8000-0000000001d1"},
	} {
		frames, err := streams.RangeSince(ctx, target, "0-0")
		require.NoError(t, err)
		require.Len(t, frames, 1, "%+v", target)
		assert.Equal(t, "workflow.WorkflowTaskCreated", frames[0].EventType, "the SSE event: line carries this name (%+v)", target)
	}
}

func TestFanout_AnLLMEventReachesItsRecipientsAndNotTheTenantTier(t *testing.T) {
	client := newClient(t)
	routes, err := routingfile.Load("../../../config/routing.yaml")
	require.NoError(t, err)
	fanout := usecase.NewFanoutService(valkey.NewEventStore(client, 10*time.Minute, 15*time.Minute), routes, usecase.CallerCheckOff)
	streams := valkey.NewStreamStore(client)
	ctx := context.Background()

	name, err := fanout.Ingest(ctx, usecase.IngestInput{
		Source:      "llm-retrieval-generation",
		EventType:   "GenerationProgressed",
		TenantID:    "t-llm",
		Payload:     []byte(`{"recipients":[{"kind":"user","id":"00000000-0000-4000-8000-0000000002a1"},{"kind":"department","id":"00000000-0000-4000-8000-0000000002d1"}]}`),
		RawEnvelope: []byte(`{"type":"GenerationProgressed"}`),
	})
	require.NoError(t, err)
	assert.Equal(t, "llm.GenerationProgressed", name)

	for _, target := range []usecase.StreamTarget{
		{TenantID: "t-llm", UserID: "00000000-0000-4000-8000-0000000002a1"},
		{TenantID: "t-llm", DepartmentID: "00000000-0000-4000-8000-0000000002d1"},
	} {
		frames, err := streams.RangeSince(ctx, target, "0-0")
		require.NoError(t, err)
		require.Len(t, frames, 1, "%+v", target)
		assert.Equal(t, "llm.GenerationProgressed", frames[0].EventType, "%+v", target)
	}
	tenant, err := streams.RangeSince(ctx, usecase.StreamTarget{TenantID: "t-llm"}, "0-0")
	require.NoError(t, err)
	assert.Empty(t, tenant, "LLM events do not reach the tenant administrators")
}
