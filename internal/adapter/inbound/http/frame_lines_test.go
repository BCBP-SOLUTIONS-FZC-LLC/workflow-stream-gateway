package http

import (
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGinFrameSink_APayloadWithLineBreaksStaysOneEvent(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	sink := ginFrameSink{w: c.Writer}

	require.NoError(t, sink.WriteFrame("k=1-0", "workflow.WorkflowTaskCreated", []byte("{\n  \"a\": 1,\r\n  \"b\": 2\r}")))
	require.NoError(t, sink.WriteControlFrame("resync-required", []byte("{\n}")))

	assert.Equal(t,
		"id: k=1-0\nevent: workflow.WorkflowTaskCreated\ndata: {\ndata:   \"a\": 1,\ndata:   \"b\": 2\ndata: }\n\n"+
			"event: resync-required\ndata: {\ndata: }\n\n",
		rec.Body.String(),
		"each line of the payload is its own data field, so no line can start a new field or end the event")
}
