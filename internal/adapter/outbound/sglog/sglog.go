// Package sglog adapts this service's existing *slog.Logger onto the
// 4-method shape (Debug/Info/Warn/Error(msg string, fields
// map[string]interface{})) that platform-gincommon's gincommon.Config.Logger
// requires — a nil Logger there disables all of gincommon's own request/
// panic logging (its own doc comment). This service has no port.Logger-
// shaped interface of its own (unlike execution_service/definition_service,
// whose flatter internal/core/port doesn't exist here per .go-arch-lint.yml's
// component list), so this adapter exists purely to satisfy gincommon's
// structurally-typed interface without this service adopting a new logging
// library or replacing its existing log/slog call sites in cmd/server.
package sglog

import "log/slog"

// Adapter wraps a *slog.Logger to satisfy gincommon.Config.Logger.
type Adapter struct {
	logger *slog.Logger
}

// New wraps logger. logger must not be nil — pass slog.Default() (or the
// *slog.Logger already constructed in cmd/server's main()) rather than a
// nil value; a nil Adapter.logger would panic on the first log call, the
// same contract *slog.Logger's own package-level functions already have.
func New(logger *slog.Logger) *Adapter {
	return &Adapter{logger: logger}
}

func (a *Adapter) Debug(msg string, fields map[string]interface{}) {
	a.logger.Debug(msg, toArgs(fields)...)
}

func (a *Adapter) Info(msg string, fields map[string]interface{}) {
	a.logger.Info(msg, toArgs(fields)...)
}

func (a *Adapter) Warn(msg string, fields map[string]interface{}) {
	a.logger.Warn(msg, toArgs(fields)...)
}

func (a *Adapter) Error(msg string, fields map[string]interface{}) {
	a.logger.Error(msg, toArgs(fields)...)
}

// toArgs flattens a fields map into slog's alternating key/value argument
// list — map iteration order is randomized, but slog attributes are
// unordered key/value pairs anyway, so this costs nothing observable.
func toArgs(fields map[string]interface{}) []any {
	args := make([]any, 0, len(fields)*2)
	for k, v := range fields {
		args = append(args, k, v)
	}
	return args
}
