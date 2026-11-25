package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway/internal/usecase"
)

type flushWatchingStore struct {
	stopAfterOneStreamStore
	rec                *httptest.ResponseRecorder
	flushedAtFirstRead *bool
}

func (s *flushWatchingStore) ReadNew(ctx context.Context, positions []usecase.StreamPosition, block time.Duration) ([]domain.Frame, error) {
	if s.flushedAtFirstRead == nil {
		flushed := s.rec.Flushed
		s.flushedAtFirstRead = &flushed
	}
	return s.stopAfterOneStreamStore.ReadNew(ctx, positions, block)
}

func TestStreamHandler_Stream_TheHeadersReachTheClientBeforeTheFirstWait(t *testing.T) {
	ticketStore := &fakeTicketStore{tickets: map[string]domain.Ticket{"tok": {TenantID: testTenantID, UserID: testUserID}}}
	rec := httptest.NewRecorder()
	store := &flushWatchingStore{rec: rec}
	r := newStreamTestRouter(ticketStore, store, &alwaysAllowLimiter{allow: true})

	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/stream?ticket=tok", nil))

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, store.flushedAtFirstRead, "the stream reached its first read")
	assert.True(t, *store.flushedAtFirstRead, "a browser's EventSource opens only when the headers arrive; without a flush they wait for the first heartbeat")
}
