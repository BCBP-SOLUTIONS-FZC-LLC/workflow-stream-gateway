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
	fanout := usecase.NewFanoutService(valkey.NewEventStore(client, 10*time.Minute, 15*time.Minute), routes)
	streams := valkey.NewStreamStore(client)
	ctx := context.Background()

	name, err := fanout.Ingest(ctx, usecase.IngestInput{
		Source:      "workflow-execution-svc",
		EventType:   "WorkflowTaskCreated",
		TenantID:    "t-routing",
		Payload:     []byte(`{"assignee_user_ids":["u-routing"],"department_id":"d-routing"}`),
		RawEnvelope: []byte(`{"type":"WorkflowTaskCreated"}`),
	})
	require.NoError(t, err)
	assert.Equal(t, "workflow.WorkflowTaskCreated", name)

	for _, target := range []usecase.StreamTarget{
		{TenantID: "t-routing", UserID: "u-routing"},
		{TenantID: "t-routing"},
		{TenantID: "t-routing", DepartmentID: "d-routing"},
	} {
		frames, err := streams.RangeSince(ctx, target, "0-0")
		require.NoError(t, err)
		require.Len(t, frames, 1, "%+v", target)
		assert.Equal(t, "workflow.WorkflowTaskCreated", frames[0].EventType, "the SSE event: line carries this name (%+v)", target)
	}
}
