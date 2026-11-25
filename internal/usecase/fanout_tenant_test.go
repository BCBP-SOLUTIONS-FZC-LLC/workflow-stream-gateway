package usecase

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
)

type tenantRecordingStore struct{ writes int }

func (s *tenantRecordingStore) AppendEvent(context.Context, string, string, string, []byte) error {
	s.writes++
	return nil
}

func (s *tenantRecordingStore) AppendBroadcastEvent(context.Context, string, string, []byte) error {
	s.writes++
	return nil
}

func (s *tenantRecordingStore) AppendDepartmentEvent(context.Context, string, string, string, []byte) error {
	s.writes++
	return nil
}

func TestFanoutService_Ingest_RefusesATenantThatIsNotACanonicalID(t *testing.T) {
	routes, err := domain.NewRoutingTable(domain.RoutingFile{Version: 1, Services: map[string]domain.ServiceRoutes{
		"workflow": {Source: "workflow-execution-svc", Callers: []string{"ns/sa"}, TenantAdmins: boolPtrFanout(true), Events: []string{"WorkflowTaskCreated"}},
	}})
	require.NoError(t, err)
	payload := []byte(`{"recipients":[{"kind":"user","id":"00000000-0000-4000-8000-0000000000b1"}]}`)

	for _, tenant := range []string{"", "broadcast:00000000-0000-4000-8000-0000000000a1", "00000000-0000-4000-8000-0000000000A1", "t1"} {
		t.Run(tenant, func(t *testing.T) {
			store := &tenantRecordingStore{}
			svc := NewFanoutService(store, routes, CallerCheckOff)

			_, err := svc.Ingest(context.Background(), IngestInput{Source: "workflow-execution-svc", EventType: "WorkflowTaskCreated", TenantID: tenant, Payload: payload})

			assert.ErrorIs(t, err, ErrInvalidPayload)
			assert.Zero(t, store.writes, "a tenant id outside the canonical form could name another tenant's key")
		})
	}
}

func boolPtrFanout(b bool) *bool { return &b }
