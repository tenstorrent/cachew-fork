package git

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/alecthomas/errors"

	"github.com/tenstorrent/internal-cachew/internal/gitclone"
	"github.com/tenstorrent/internal-cachew/internal/gitobject"
)

// Exports unexported symbols for use by external test packages.

func IsGitRequest(pathValue string) bool { return isGitRequest(pathValue) }

// SetServiceToken installs a stand-in for cachew's own GitHub App credentials,
// so tests can observe which upstream requests they are attached to without a
// real GitHub App.
func (s *Strategy) SetServiceToken(resolve func(ctx context.Context, org string) (string, error)) {
	s.serviceToken = resolve
}

func (s *Strategy) GenerateAndUploadSnapshot(ctx context.Context, repo *gitclone.Repository) error {
	_, err := s.generateAndUploadSnapshot(ctx, repo)
	return err
}

func (s *Strategy) GenerateAndUploadMirrorSnapshot(ctx context.Context, repo *gitclone.Repository) error {
	_, err := s.generateAndUploadMirrorSnapshot(ctx, repo)
	return err
}

// RunCoordinatedSnapshot exports the coordinated base-snapshot job for tests.
func (s *Strategy) RunCoordinatedSnapshot(ctx context.Context, repo *gitclone.Repository, interval time.Duration) error {
	return s.coordinatedSnapshotJob(snapshotJobBase, repo, interval, func(ctx context.Context) (string, error) {
		return s.generateAndUploadSnapshot(ctx, repo)
	})(ctx)
}

// RunFailingCoordinatedSnapshot causes a snapshot job to fail so tests can check claim release.
func (s *Strategy) RunFailingCoordinatedSnapshot(ctx context.Context, repo *gitclone.Repository) error {
	return s.coordinatedSnapshotJob(snapshotJobBase, repo, 0, func(context.Context) (string, error) {
		return "", errors.New("snapshot generation failed")
	})(ctx)
}

// CacheBundle exports cacheBundle for testing.
func (s *Strategy) CacheBundle(ctx context.Context, bundle gitobject.Object, r io.Reader) error {
	return s.cacheBundle(ctx, bundle, r)
}

// IsUploadPackPost exports isUploadPackPost for testing.
func IsUploadPackPost(r *http.Request, pathValue string) bool {
	return isUploadPackPost(r, pathValue)
}

// IncrementalParse exports incrementalParse for testing.
func IncrementalParse(r *http.Request, pathValue string, body []byte) ([]string, bool) {
	return incrementalParse(r, pathValue, body)
}

// UploadPackParseLimit exports uploadPackParseLimit for testing.
const UploadPackParseLimit = uploadPackParseLimit

// SubmitFetch exports submitFetch for testing.
func SubmitFetch(s *Strategy, repo *gitclone.Repository) { s.submitFetch(repo) }

// SubmitFetchForce exports submitFetchForce for testing.
func SubmitFetchForce(s *Strategy, repo *gitclone.Repository) { s.submitFetchForce(repo) }

// EnsureWantsAvailable exports ensureWantsAvailable for testing.
func EnsureWantsAvailable(ctx context.Context, s *Strategy, repo *gitclone.Repository, wants []string) (string, error) {
	return s.ensureWantsAvailable(ctx, repo, wants)
}

// IncrementalOutcomeAfterNotOurRef exports incrementalOutcomeAfterNotOurRef for testing.
func IncrementalOutcomeAfterNotOurRef(outcome string) string {
	return incrementalOutcomeAfterNotOurRef(outcome)
}

// MirrorPreparationScheduled lets tests check for a queued or active preparation job.
func (s *Strategy) MirrorPreparationScheduled(upstream string) bool {
	_, ok := s.mirrorPreparations.Load(upstream)
	return ok
}

// PeriodicJobsScheduled lets tests check the periodic jobs that the strategy registered.
func (s *Strategy) PeriodicJobsScheduled(upstream string) (snapshot, repack bool) {
	_, snapshot = s.snapshotJobsScheduled.Load(upstream)
	_, repack = s.repackJobsScheduled.Load(upstream)
	return snapshot, repack
}

// SetColdPreparationDelay lets tests control the delay before a snapshot claim.
func (s *Strategy) SetColdPreparationDelay(delay func() time.Duration) {
	s.coldPreparationDelay = delay
}

// ColdPreparationQueueLimitForTest returns the queue limit for tests in external packages.
func ColdPreparationQueueLimitForTest() int {
	return coldPreparationQueueLimit
}

// ClaimSnapshotForTest lets tests request a base snapshot claim.
func (s *Strategy) ClaimSnapshotForTest(upstream string) (bool, error) {
	return s.snapshotCoord.Claim(snapshotJobBase, upstream, 0)
}
