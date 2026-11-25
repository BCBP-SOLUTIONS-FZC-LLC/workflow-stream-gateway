package usecase

import (
	"strconv"
	"strings"
)

const (
	cursorPairSep = "|"
	cursorKVSep   = "="
)

const NoEntryID = "0-0"

func encodeResumeCursor(positions map[string]string) string {
	resumable := make(map[string]string, len(positions))
	for key, id := range positions {
		if id != NoEntryID {
			resumable[key] = id
		}
	}
	return encodeCursor(resumable)
}

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

func isStreamID(id string) bool {
	ms, seq, hasSeq := strings.Cut(id, "-")
	if !isUint64(ms) {
		return false
	}
	return !hasSeq || isUint64(seq)
}

func isUint64(s string) bool {
	if s == "" || strings.TrimLeft(s, "0123456789") != "" {
		return false
	}
	_, err := strconv.ParseUint(s, 10, 64)
	return err == nil
}

func decodeCursor(raw string) map[string]string {
	if raw == "" {
		return nil
	}
	pairs := strings.Split(raw, cursorPairSep)
	cursors := make(map[string]string, len(pairs))
	for _, p := range pairs {
		key, id, ok := strings.Cut(p, cursorKVSep)
		if !ok || key == "" || !isStreamID(id) {
			continue
		}
		cursors[key] = id
	}
	return cursors
}
