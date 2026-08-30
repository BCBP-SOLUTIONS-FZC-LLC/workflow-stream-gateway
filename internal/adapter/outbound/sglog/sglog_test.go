package sglog

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingHandler captures every emitted slog.Record for direct assertion,
// rather than parsing formatted text/JSON output.
type recordingHandler struct {
	records []slog.Record
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.records = append(h.records, r)
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

func attrMap(r slog.Record) map[string]any {
	out := make(map[string]any, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		out[a.Key] = a.Value.Any()
		return true
	})
	return out
}

func TestAdapter_Debug_ForwardsMessageAndFields(t *testing.T) {
	h := &recordingHandler{}
	a := New(slog.New(h))

	a.Debug("debug msg", map[string]interface{}{"key": "value"})

	require.Len(t, h.records, 1)
	assert.Equal(t, slog.LevelDebug, h.records[0].Level)
	assert.Equal(t, "debug msg", h.records[0].Message)
	assert.Equal(t, map[string]any{"key": "value"}, attrMap(h.records[0]))
}

func TestAdapter_Info_ForwardsMessageAndFields(t *testing.T) {
	h := &recordingHandler{}
	a := New(slog.New(h))

	a.Info("info msg", map[string]interface{}{"count": int64(3)})

	require.Len(t, h.records, 1)
	assert.Equal(t, slog.LevelInfo, h.records[0].Level)
	assert.Equal(t, "info msg", h.records[0].Message)
	assert.Equal(t, map[string]any{"count": int64(3)}, attrMap(h.records[0]))
}

func TestAdapter_Warn_ForwardsMessageAndFields(t *testing.T) {
	h := &recordingHandler{}
	a := New(slog.New(h))

	a.Warn("warn msg", map[string]interface{}{"retry": true})

	require.Len(t, h.records, 1)
	assert.Equal(t, slog.LevelWarn, h.records[0].Level)
	assert.Equal(t, "warn msg", h.records[0].Message)
	assert.Equal(t, map[string]any{"retry": true}, attrMap(h.records[0]))
}

func TestAdapter_Error_ForwardsMessageAndFields(t *testing.T) {
	h := &recordingHandler{}
	a := New(slog.New(h))

	a.Error("error msg", map[string]interface{}{"err": "boom"})

	require.Len(t, h.records, 1)
	assert.Equal(t, slog.LevelError, h.records[0].Level)
	assert.Equal(t, "error msg", h.records[0].Message)
	assert.Equal(t, map[string]any{"err": "boom"}, attrMap(h.records[0]))
}

func TestAdapter_NilFields_LogsWithNoAttrs(t *testing.T) {
	h := &recordingHandler{}
	a := New(slog.New(h))

	a.Info("no fields", nil)

	require.Len(t, h.records, 1)
	assert.Equal(t, 0, h.records[0].NumAttrs())
}
