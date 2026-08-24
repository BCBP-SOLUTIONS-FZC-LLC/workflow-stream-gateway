package usecase

import "strings"

// A cursor is the SSE resume token a reconnecting client's Last-Event-ID
// echoes back (LLD §8). A connection tailing more than one Valkey Stream
// at once (personal plus tenant-wide, or personal plus one or more
// departments) needs that single opaque string to carry a resume
// position for every stream it tails, not just whichever stream emitted
// the most recent frame — EventSource remembers exactly one
// Last-Event-ID value, never one per source.
//
// Encoded as "key1=id1|key2=id2", keyed by each target's own Valkey key
// (streamstore.keyFor) — Valkey Stream IDs are digits/hyphen only and
// keyFor's keys use ':', so neither collides with '=' or '|'.
const (
	cursorPairSep = "|"
	cursorKVSep   = "="
)

func encodeCursor(cursors map[string]string) string {
	if len(cursors) == 0 {
		return ""
	}
	pairs := make([]string, 0, len(cursors))
	for key, id := range cursors {
		pairs = append(pairs, key+cursorKVSep+id)
	}
	return strings.Join(pairs, cursorPairSep)
}

// decodeCursor parses a compound cursor into key->id. A malformed pair
// (missing '=') is skipped rather than erroring — a garbled or
// hand-edited Last-Event-ID should degrade to "no backfill for that
// target," the same as a key genuinely absent from the string, not fail
// the whole reconnect. Which keys are actually meaningful is decided
// entirely by the caller (StreamService.backfill, against the ticket's
// own streamTargets) — decodeCursor never trusts a key it's given as
// proof that target is real.
func decodeCursor(raw string) map[string]string {
	if raw == "" {
		return nil
	}
	pairs := strings.Split(raw, cursorPairSep)
	cursors := make(map[string]string, len(pairs))
	for _, p := range pairs {
		key, id, ok := strings.Cut(p, cursorKVSep)
		if !ok || key == "" || id == "" {
			continue
		}
		cursors[key] = id
	}
	return cursors
}
