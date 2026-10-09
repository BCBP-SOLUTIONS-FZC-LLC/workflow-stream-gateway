package usecase

import "strings"

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
