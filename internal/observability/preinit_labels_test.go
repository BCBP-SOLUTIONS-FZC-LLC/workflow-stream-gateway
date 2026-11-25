package observability_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/observability/obstest"
)

func TestRegister_InternalAuthRejectedReasonsExposedAtZeroAndOpenSetsNot(t *testing.T) {
	obstest.Init(t)

	gathered, err := gincommon.MetricsGatherer().Gather()
	require.NoError(t, err)
	series := map[string]map[string]float64{}
	for _, mf := range gathered {
		for _, m := range mf.GetMetric() {
			if series[mf.GetName()] == nil {
				series[mf.GetName()] = map[string]float64{}
			}
			key := ""
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "reason" {
					key = lp.GetValue()
				}
			}
			series[mf.GetName()][key] = m.GetCounter().GetValue()
		}
	}

	rejected := series["workflow_stream_gateway_internal_auth_rejected_total"]
	for _, reason := range []string{"missing_user", "missing_role", "caller_not_allowed"} {
		v, ok := rejected[reason]
		assert.True(t, ok, reason)
		assert.Zero(t, v, reason)
	}
	assert.Empty(t, series["workflow_stream_gateway_internal_events_ingest_total"])
	assert.Empty(t, series["workflow_stream_gateway_frames_written_total"])
}
