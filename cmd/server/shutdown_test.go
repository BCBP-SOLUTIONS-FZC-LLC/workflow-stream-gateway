package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestShutdownTimeout_OutlastsOneHeartbeat(t *testing.T) {
	assert.Equal(t, 10*time.Second, shutdownTimeout(time.Second), "never below the floor")
	assert.Equal(t, 20*time.Second, shutdownTimeout(15*time.Second), "an open stream wakes within one heartbeat, then ends")
}
