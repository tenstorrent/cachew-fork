package git_test

import (
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/tenstorrent/internal-cachew/internal/strategy/git"
)

func TestIsStaleCommitGraphError(t *testing.T) {
	// Captured from git 2.54 against a mirror whose commit-graph named a commit
	// the object store no longer had.
	const staleGraph = "error: Could not read 4f2b36ef1f9c6dd62239a7817ef3b92ab5b8dbcf\n" +
		"failed to parse commit 4f2b36ef1f9c6dd62239a7817ef3b92ab5b8dbcf from object database for commit-graph\n"

	// The same broken index, served through upload-pack with
	// uploadpack.allowAnySHA1InWant, which is what cachew configures.
	const serveFailure = "error: git upload-pack: git-pack-objects died with error.\n" +
		"fatal: git upload-pack: aborting due to possible repository corruption on the remote side.\n"

	for _, tc := range []struct {
		name   string
		stderr string
		want   bool
	}{
		{name: "missing commit via commit-graph", stderr: staleGraph, want: true},
		{
			name:   "unable to parse commit",
			stderr: "fatal: unable to parse commit 4f2b36ef1f9c6dd62239a7817ef3b92ab5b8dbcf",
			want:   true,
		},
		{name: "upload-pack corruption from the stale graph", stderr: serveFailure, want: true},
		{name: "clean stderr", stderr: "", want: false},
		{
			name:   "not our ref is a different fault",
			stderr: "error: git upload-pack: not our ref 4f2b36ef1f9c6dd62239a7817ef3b92ab5b8dbcf",
			want:   false,
		},
		{
			name:   "unrelated object read failure",
			stderr: "error: Could not read 4f2b36ef1f9c6dd62239a7817ef3b92ab5b8dbcf\nfatal: loose object is corrupt",
			want:   false,
		},
		{
			name:   "a genuine pack failure is not a graph fault",
			stderr: "error: git upload-pack: git-pack-objects died with error.\nfatal: out of memory",
			want:   false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, git.IsStaleCommitGraphError(tc.stderr))
		})
	}
}
