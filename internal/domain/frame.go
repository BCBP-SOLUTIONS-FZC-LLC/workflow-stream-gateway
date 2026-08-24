package domain

// Frame is one Valkey Stream entry, read back for either live-tail
// delivery or reconnect backfill (LLD §7, §8). ID is the Stream entry ID
// (the SSE resume token), distinct from the UUID inside Data (the
// envelope's own id). SourceKey is the Valkey key the entry came from —
// needed to update the right slot of a connection's compound
// per-target resume cursor (LLD §8) when a connection tails more than
// one stream at once.
type Frame struct {
	ID        string
	EventType string
	Data      []byte
	SourceKey string
}
