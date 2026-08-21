package domain

// Frame is one Valkey Stream entry, read back for either live-tail
// delivery or reconnect backfill (LLD §7, §8). ID is the Stream entry ID
// (the SSE resume token), distinct from the UUID inside Data (the
// envelope's own id).
type Frame struct {
	ID        string
	EventType string
	Data      []byte
}
