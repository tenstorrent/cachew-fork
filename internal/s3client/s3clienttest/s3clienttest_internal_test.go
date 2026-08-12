package s3clienttest

import (
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
)

func TestDockerAvailableFalseWhenDockerNotOnPath(t *testing.T) {
	t.Setenv("PATH", t.TempDir())

	start := time.Now()
	available := dockerAvailable(t)
	elapsed := time.Since(start)

	assert.False(t, available)
	assert.True(t, elapsed < dockerProbeTimeout,
		"dockerAvailable took %s, should fail well within its own timeout when the binary is simply missing", elapsed)
}
