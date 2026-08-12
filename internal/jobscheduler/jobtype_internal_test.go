package jobscheduler

import (
	"testing"

	"github.com/alecthomas/assert/v2"
)

func TestJobTypeNamesEveryJobTheGitStrategySubmits(t *testing.T) {
	for id, want := range map[string]string{
		"clone":                    jobTypeClone,
		"deferred-mirror-restore":  jobTypeClone,
		"fetch":                    jobTypeFetch,
		"snapshot-periodic":        jobTypeSnapshot,
		"lfs-snapshot-periodic":    jobTypeSnapshot,
		"mirror-snapshot-periodic": jobTypeSnapshot,
		"repack-periodic":          jobTypeRepack,
		"prune-periodic":           jobTypePrune,
		"startup-maintenance":      jobTypeStartupMaintenance,
		"reap-repo-counts":         jobTypeOther,
	} {
		assert.Equal(t, want, jobType(id), "job %q", id)
	}
}
