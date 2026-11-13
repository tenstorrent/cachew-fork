package git //nolint:testpackage // white-box testing required for the unexported backend fallback reasons

import (
	"testing"

	"github.com/alecthomas/assert/v2"
)

func TestIncrementalOutcomeAfterServe(t *testing.T) {
	tests := []struct {
		name     string
		outcome  string
		fallback backendFallback
		served   bool
		want     string
	}{
		{name: "IncrementalDidNotRun", fallback: backendNotOurRef, want: ""},
		{name: "ServedFirstTime", outcome: "local_hit", fallback: backendServed, served: true, want: "local_hit"},
		{name: "ServedAfterGraphRewrite", outcome: "local_hit", fallback: backendStaleCommitGraph, served: true,
			want: "local_hit"},
		{name: "GraphRewriteFellBack", outcome: "fetched", fallback: backendStaleCommitGraph,
			want: "fallback_stale_commit_graph"},
		{name: "ServedAfterNotOurRefFetch", outcome: "local_hit", fallback: backendNotOurRef, served: true,
			want: "fetched"},
		{name: "NotOurRefFellBack", outcome: "fetched", fallback: backendNotOurRef, want: "fallback_not_our_ref"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, incrementalOutcomeAfterServe(tt.outcome, tt.fallback, tt.served))
		})
	}
}
