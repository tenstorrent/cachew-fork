package git_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/tenstorrent/internal-cachew/internal/gitclone"
	"github.com/tenstorrent/internal-cachew/internal/strategy/git"
)

func TestUploadPackPackThreadBudgetOverridesMirrorPackThreads(t *testing.T) {
	tests := []struct {
		name     string
		budget   int
		expected string
	}{
		{name: "BudgetBelowPackThreads", budget: 3, expected: "3"},
		{name: "BudgetDisabled", budget: 0, expected: "8"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newInfoRefsFixtureWithConfig(t, git.Config{DisableUpstreamAuthz: true, PackThreadBudget: tt.budget},
				gitclone.Config{PackThreads: 8})
			setInfoRefsLsRemote(t, fixture.upstreamPath, false)
			runInfoRefsGit(t, "-C", fixture.mirrorPath, "config", "pack.threads", "8")
			head := runInfoRefsGit(t, "-C", fixture.mirrorPath, "rev-parse", "refs/heads/main")
			trace := traceGitOnPath(t)

			w := fixture.requestUploadPackWithCaps(t, head)

			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, tt.expected, effectivePackThreads(t, trace, "pack-objects"))
		})
	}
}

// traceGitOnPath puts a git wrapper first on PATH that turns on trace2 event
// logging of pack.threads, so every git process spawned below cachew, down to
// pack-objects, reports the value it resolved. It returns the trace file.
func traceGitOnPath(t *testing.T) string {
	t.Helper()
	realGit, err := exec.LookPath("git")
	assert.NoError(t, err)
	dir := t.TempDir()
	trace := filepath.Join(dir, "trace2.json")
	script := fmt.Sprintf(`#!/bin/sh
GIT_TRACE2_EVENT=%q GIT_TRACE2_CONFIG_PARAMS=pack.threads exec %q "$@"
`, trace, realGit)
	assert.NoError(t, os.WriteFile(filepath.Join(dir, "git"), []byte(script), 0o700)) //nolint:gosec // owner-only, but must be executable
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return trace
}

// effectivePackThreads returns the last pack.threads value trace2 reported for
// the named git command; git reports each scope in precedence order, so the last
// one is the value the command used.
func effectivePackThreads(t *testing.T, trace, command string) string {
	t.Helper()
	f, err := os.Open(trace) //nolint:gosec // test-owned temp path
	assert.NoError(t, err)
	defer f.Close()

	type event struct {
		Event string `json:"event"`
		SID   string `json:"sid"`
		Name  string `json:"name"`
		Param string `json:"param"`
		Value string `json:"value"`
	}
	names := map[string]string{}
	values := map[string]string{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var e event
		assert.NoError(t, json.Unmarshal(scanner.Bytes(), &e))
		switch {
		case e.Event == "cmd_name":
			names[e.SID] = e.Name
		case e.Event == "def_param" && e.Param == "pack.threads":
			values[e.SID] = e.Value
		}
	}
	assert.NoError(t, scanner.Err())
	for sid, name := range names {
		if name == command {
			return values[sid]
		}
	}
	t.Fatalf("no %s process in trace", command)
	return ""
}
