package git_test

import (
	"context"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/tenstorrent/internal-cachew/internal/gitclone"
	"github.com/tenstorrent/internal-cachew/internal/githubapp"
	"github.com/tenstorrent/internal-cachew/internal/logging"
	"github.com/tenstorrent/internal-cachew/internal/strategy/git"
)

func TestRepackInterval(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	tests := []struct {
		name           string
		repackInterval time.Duration
	}{
		{
			name:           "Enabled",
			repackInterval: 24 * time.Hour,
		},
		{
			name:           "Disabled",
			repackInterval: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := newTestMux()
			cm := gitclone.NewManagerProvider(ctx, gitclone.Config{
				MirrorRoot: filepath.Join(tmpDir, tt.name),
			}, nil)
			s, err := git.New(ctx, git.Config{
				RepackInterval: tt.repackInterval,
			}, newTestScheduler(ctx, t), nil, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }, nil) //nolint:nilnil
			assert.NoError(t, err)
			assert.True(t, s != nil)
			waitForReady(t, s)
		})
	}
}

func TestRepackScheduledForExistingRepos(t *testing.T) {
	_, ctx, _ := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	clonePath := filepath.Join(tmpDir, "github.com", "org", "repo")
	output, err := exec.Command("git", "init", "--bare", clonePath).CombinedOutput()
	assert.NoError(t, err, string(output))

	mux := newTestMux()
	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{
		MirrorRoot: tmpDir,
	}, nil)
	manager, err := cm()
	assert.NoError(t, err)
	const upstreamURL = "https://github.com/org/repo"
	_, err = manager.GetOrCreate(ctx, upstreamURL)
	assert.NoError(t, err)
	output, err = exec.Command("git", "-C", clonePath, "config", "remote.origin.url", clonePath).CombinedOutput()
	assert.NoError(t, err, string(output))
	s, err := git.New(ctx, git.Config{
		RepackInterval: 24 * time.Hour, DisableUpstreamAuthz: true,
	}, newTestScheduler(ctx, t), nil, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }, nil) //nolint:nilnil
	assert.NoError(t, err)
	waitForReady(t, s)
	_, repack := s.PeriodicJobsScheduled(upstreamURL)
	assert.True(t, repack)
}
