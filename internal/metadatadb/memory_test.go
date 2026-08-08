package metadatadb_test

import (
	"testing"

	"github.com/tenstorrent/internal-cachew/internal/metadatadb"
	"github.com/tenstorrent/internal-cachew/internal/metadatadb/metadatadbtest"
)

func TestMemoryBackend(t *testing.T) {
	metadatadbtest.Suite(t, func(t *testing.T, n int) []metadatadb.Backend {
		t.Helper()
		backend := metadatadb.NewMemoryBackend()
		backends := make([]metadatadb.Backend, n)
		for i := range backends {
			backends[i] = backend
		}
		return backends
	})
}
