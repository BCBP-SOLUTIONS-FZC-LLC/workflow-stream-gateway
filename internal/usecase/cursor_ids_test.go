package usecase

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
)

func TestDecodeCursor_KeepsOnlyWellFormedStreamIDs(t *testing.T) {
	raw := "a=1-0|b=9999999999999-x|c=$|d=+|e=-|f=12|g=18446744073709551616-0|h=7-18446744073709551615"

	assert.Equal(t, map[string]string{"a": "1-0", "f": "12", "h": "7-18446744073709551615"}, decodeCursor(raw))
}

func TestStreamService_Serve_AResumeIDBeyondTheNewestEntryStartsAtTheNewest(t *testing.T) {
	store := &fakeStreamStore{latestID: "50-0", oldestID: "10-0", oldestExists: true}
	store.readNewSeq = []readNewResult{{}}

	err := newTestStreamService(store, &fakeLimiter{}).Serve(context.Background(), testTicket(), compoundID(testPersonalKey, "99999999999999-0"), &fakeSink{})

	assert.ErrorIs(t, err, errStopTest)
	require.NotEmpty(t, store.readNewPositions)
	assert.Equal(t, "50-0", store.readNewPositions[0][0].AfterID, "an id no entry can reach would leave the connection silent")
}

func TestStreamService_Serve_AMalformedResumeIDIsAFreshStart(t *testing.T) {
	store := &fakeStreamStore{latestID: "50-0", oldestID: "10-0", oldestExists: true, rangeFrames: []domain.Frame{{ID: "30-0", EventType: "E"}}}
	store.readNewSeq = []readNewResult{{}}
	sink := &fakeSink{}

	err := newTestStreamService(store, &fakeLimiter{}).Serve(context.Background(), testTicket(), compoundID(testPersonalKey, "9999999999999-x"), sink)

	assert.ErrorIs(t, err, errStopTest)
	assert.Empty(t, sink.frames, "no backfill for an id that is not a stream id")
	assert.Empty(t, sink.control, "no resync either")
	assert.Equal(t, "50-0", store.readNewPositions[0][0].AfterID)
}
