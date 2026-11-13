package gitclone

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/tenstorrent/internal-cachew/internal/transfer"
)

// packFileSizes returns the size of each pack file in the repository at
// repoPath, keyed by file name. A missing or unreadable pack directory yields
// no entries: the sizes only feed a metric, which must never fail a clone or
// fetch.
func packFileSizes(repoPath string) map[string]int64 {
	entries, err := os.ReadDir(filepath.Join(repoPath, "objects", "pack"))
	if err != nil {
		return nil
	}
	sizes := make(map[string]int64, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".pack") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		sizes[entry.Name()] = info.Size()
	}
	return sizes
}

// recordReceivedPacks counts the packs in after that were not in before as pack
// data received from upstream.
//
// Mirrors keep every received pack as written (transfer.unpackLimit=1, and
// clone never unpacks), and fetches hold the fetch exclusion that repack and
// prune also take, so a pack that appeared during the transfer is the transfer.
// A thin pack is completed with its base objects on receipt, so this slightly
// overstates the bytes on the wire.
func recordReceivedPacks(ctx context.Context, before, after map[string]int64) {
	var received int64
	for name, size := range after {
		if _, existed := before[name]; !existed {
			received += size
		}
	}
	transfer.RecordReceived(ctx, transfer.SourceGit, "git", received)
}
