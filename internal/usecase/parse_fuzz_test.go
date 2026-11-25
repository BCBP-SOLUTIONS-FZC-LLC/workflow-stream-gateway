package usecase

import (
	"errors"
	"strings"
	"testing"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
)

func FuzzDecodeCursor_KeepsOnlyCleanPairs(f *testing.F) {
	for _, seed := range []string{
		"", "k=1-0", "a=1-0|b=2-3", "a=1-0|bad|=5|k2=", "k=9999999999999-x", "k=$|l=+|m=-",
		"k=18446744073709551615-18446744073709551615", "k=18446744073709551616-0", "k==1-0", "|||", "k=1-0-0",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		for key, id := range decodeCursor(raw) {
			if key == "" || strings.Contains(key, cursorPairSep) {
				t.Fatalf("key %q kept from %q", key, raw)
			}
			if !isStreamID(id) {
				t.Fatalf("id %q kept from %q", id, raw)
			}
		}
		positions := decodeCursor(raw)
		if again := decodeCursor(encodeCursor(positions)); len(again) != len(positions) {
			t.Fatalf("round trip of %q changed %v to %v", raw, positions, again)
		}
	})
}

func FuzzCompareStreamIDs_IsAnOrder(f *testing.F) {
	f.Add("1-0", "1-1")
	f.Add("9-0", "10-0")
	f.Add("18446744073709551615-0", "0-18446744073709551615")
	f.Fuzz(func(t *testing.T, a, b string) {
		if !isStreamID(a) || !isStreamID(b) {
			return
		}
		if compareStreamIDs(a, b) != -compareStreamIDs(b, a) {
			t.Fatalf("compare(%q, %q) is not antisymmetric", a, b)
		}
		if compareStreamIDs(a, a) != 0 {
			t.Fatalf("compare(%q, %q) is not 0", a, a)
		}
	})
}

func FuzzResolveTarget_AcceptsOnlyCanonicalRecipients(f *testing.F) {
	for _, seed := range []string{
		`{"recipients":[]}`,
		`{"recipients":[{"kind":"user","id":"00000000-0000-4000-8000-0000000000b1"}]}`,
		`{"recipients":[{"kind":"department","id":"00000000-0000-4000-8000-0000000000c1"},{"kind":"user","id":"00000000-0000-4000-8000-0000000000c1"}]}`,
		`{"recipients":null}`, `{"recipients":[null]}`, `{"recipients":{}}`, `[]`, `null`, ``, `{"recipients":[{"kind":"user","id":"a:b"}]}`,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, payload []byte) {
		got, err := resolveTarget(payload)
		if err != nil {
			if !errors.Is(err, ErrInvalidPayload) {
				t.Fatalf("error %v for %q is not an invalid payload", err, payload)
			}
			return
		}
		for _, id := range append(append([]string{}, got.users...), got.departments...) {
			if !domain.IsCanonicalID(id) {
				t.Fatalf("recipient %q accepted from %q", id, payload)
			}
		}
	})
}
