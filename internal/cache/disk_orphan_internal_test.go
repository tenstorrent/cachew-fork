package cache

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/tenstorrent/internal-cachew/internal/logging"
)

func newDiskForOrphanTest(t *testing.T, root string) *Disk {
	t.Helper()
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
	d, err := NewDisk(ctx, DiskConfig{
		Root:          root,
		LimitMB:       10,
		MaxTTL:        time.Hour,
		EvictInterval: time.Minute,
	})
	assert.NoError(t, err)
	return d
}

// writeEntry stores one entry and returns the path it landed at.
func writeEntry(t *testing.T, d *Disk, name, payload string) (Key, string) {
	t.Helper()
	_, ctx, _ := logging.Configure(t.Context(), logging.Config{Level: "error"})
	key := NewKey(name)
	w, err := d.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write([]byte(payload))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())
	return key, filepath.Join(d.config.Root, d.keyToPath(d.namespace, key))
}

func TestDiskReclaimsFileWithNoMetadataRecord(t *testing.T) {
	root := t.TempDir()

	d := newDiskForOrphanTest(t, root)
	_, kept := writeEntry(t, d, "kept", "payload")
	orphanKey := NewKey("orphan")
	orphan := filepath.Join(root, d.keyToPath(d.namespace, orphanKey))
	assert.NoError(t, d.Close())

	// The shape a crash leaves: the entry's file is published at its key, but
	// nothing in the metadata database points at it.
	assert.NoError(t, os.MkdirAll(filepath.Dir(orphan), 0o750))
	assert.NoError(t, os.WriteFile(orphan, []byte("unreferenced"), 0o600))

	reopened := newDiskForOrphanTest(t, root)
	defer reopened.Close()

	_, err := os.Stat(orphan)
	assert.True(t, os.IsNotExist(err), "a file with no metadata record survived startup")
	_, err = os.Stat(kept)
	assert.NoError(t, err, "startup deleted a file that had a metadata record")
	assert.Equal(t, int64(len("payload")), reopened.Size(),
		"the orphan's bytes are still counted against the size limit")
}

func TestDiskReclaimsAbandonedTempFile(t *testing.T) {
	root := t.TempDir()

	d := newDiskForOrphanTest(t, root)
	_, kept := writeEntry(t, d, "kept", "payload")
	assert.NoError(t, d.Close())

	temp := filepath.Join(filepath.Dir(kept), diskTempPrefix+"abandoned")
	assert.NoError(t, os.WriteFile(temp, []byte("half-written"), 0o600))

	reopened := newDiskForOrphanTest(t, root)
	defer reopened.Close()

	_, err := os.Stat(temp)
	assert.True(t, os.IsNotExist(err), "an abandoned temp file survived startup")
	assert.Equal(t, int64(len("payload")), reopened.Size())
}

func TestDiskKeepsTempPrefixedFileOutsideShardLayout(t *testing.T) {
	root := t.TempDir()

	d := newDiskForOrphanTest(t, root)
	assert.NoError(t, d.Close())

	// Create never opens a temp file outside a shard directory, so neither of
	// these is ours to delete however its name reads.
	atRoot := filepath.Join(root, diskTempPrefix+"stray")
	assert.NoError(t, os.WriteFile(atRoot, []byte("not ours"), 0o600))
	inStrangeDir := filepath.Join(root, "somedir", diskTempPrefix+"stray")
	assert.NoError(t, os.MkdirAll(filepath.Dir(inStrangeDir), 0o750))
	assert.NoError(t, os.WriteFile(inStrangeDir, []byte("nor this"), 0o600))

	reopened := newDiskForOrphanTest(t, root)
	defer reopened.Close()

	_, err := os.Stat(atRoot)
	assert.NoError(t, err, "startup deleted a temp-prefixed file in the cache root")
	_, err = os.Stat(inStrangeDir)
	assert.NoError(t, err, "startup deleted a temp-prefixed file outside the shard layout")
	assert.Equal(t, int64(len("not ours")+len("nor this")), reopened.Size(),
		"files left in place are still counted against the size limit")
}

func TestInEntryShardDir(t *testing.T) {
	root := "/cache"
	key := NewKey("some-object")
	hexKey := key.String()

	for _, test := range []struct {
		name string
		path string
		want bool
	}{
		{name: "shard", path: filepath.Join(root, hexKey[:2], diskTempPrefix+"x"), want: true},
		{name: "namespaced shard", path: filepath.Join(root, "git", hexKey[:2], diskTempPrefix+"x"), want: true},
		{name: "cache root", path: filepath.Join(root, diskTempPrefix+"x"), want: false},
		{name: "not a shard name", path: filepath.Join(root, "somedir", diskTempPrefix+"x"), want: false},
		{name: "shard name not hex", path: filepath.Join(root, "zz", diskTempPrefix+"x"), want: false},
		{name: "too deep", path: filepath.Join(root, "a", "b", hexKey[:2], diskTempPrefix+"x"), want: false},
		{name: "outside root", path: filepath.Join("/elsewhere", hexKey[:2], diskTempPrefix+"x"), want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, inEntryShardDir(root, test.path))
		})
	}
}

func TestDiskKeepsUnrecognisedFile(t *testing.T) {
	root := t.TempDir()

	d := newDiskForOrphanTest(t, root)
	assert.NoError(t, d.Close())

	stray := filepath.Join(root, "not-a-cache-entry")
	assert.NoError(t, os.WriteFile(stray, []byte("keep me"), 0o600))

	reopened := newDiskForOrphanTest(t, root)
	defer reopened.Close()

	_, err := os.Stat(stray)
	assert.NoError(t, err, "startup deleted a file it did not recognise")
	assert.Equal(t, int64(len("keep me")), reopened.Size())
}

func TestDiskCountsNestedFileNamedLikeTheMetadataDB(t *testing.T) {
	root := t.TempDir()

	d := newDiskForOrphanTest(t, root)
	assert.NoError(t, d.Close())

	nested := filepath.Join(root, "somedir", metaDBFileName)
	assert.NoError(t, os.MkdirAll(filepath.Dir(nested), 0o750))
	assert.NoError(t, os.WriteFile(nested, []byte("not the database"), 0o600))

	reopened := newDiskForOrphanTest(t, root)
	defer reopened.Close()

	_, err := os.Stat(nested)
	assert.NoError(t, err)
	assert.Equal(t, int64(len("not the database")), reopened.Size(),
		"only the database at the cache root is exempt from the size accounting")
}

func TestDiskKeepsFileNamedLikeAKeyButNotHex(t *testing.T) {
	root := t.TempDir()

	d := newDiskForOrphanTest(t, root)
	assert.NoError(t, d.Close())

	// Right length and in a directory that agrees with its first two
	// characters, so only the hex check keeps it out of the orphan path.
	stray := filepath.Join(root, "zz", strings.Repeat("z", hexKeyLength))
	assert.NoError(t, os.MkdirAll(filepath.Dir(stray), 0o750))
	assert.NoError(t, os.WriteFile(stray, []byte("keep me"), 0o600))

	reopened := newDiskForOrphanTest(t, root)
	defer reopened.Close()

	_, err := os.Stat(stray)
	assert.NoError(t, err, "startup deleted a file whose name is not a key")
	assert.Equal(t, int64(len("keep me")), reopened.Size())
}

func TestDiskReclaimsOrphanInOtherNamespace(t *testing.T) {
	root := t.TempDir()

	d := newDiskForOrphanTest(t, root)
	namespaced, ok := d.Namespace("ns").(*Disk)
	assert.True(t, ok)
	key, kept := writeEntry(t, namespaced, "shared", "payload")
	// Same key, different namespace, no record of its own.
	orphan := filepath.Join(root, d.keyToPath("other", key))
	assert.NoError(t, d.Close())

	assert.NoError(t, os.MkdirAll(filepath.Dir(orphan), 0o750))
	assert.NoError(t, os.WriteFile(orphan, []byte("unreferenced"), 0o600))

	reopened := newDiskForOrphanTest(t, root)
	defer reopened.Close()

	_, err := os.Stat(orphan)
	assert.True(t, os.IsNotExist(err), "an orphan under an unrecorded namespace survived startup")
	_, err = os.Stat(kept)
	assert.NoError(t, err)
}

func TestEntryKeyForPath(t *testing.T) {
	root := "/cache"
	key := NewKey("some-object")
	hexKey := key.String()
	upperHex := strings.ToUpper(hexKey)

	for _, test := range []struct {
		name      string
		path      string
		wantOK    bool
		wantNS    Namespace
		wantKeyed bool
	}{
		{name: "namespaced", path: filepath.Join(root, "git", hexKey[:2], hexKey), wantOK: true, wantNS: "git", wantKeyed: true},
		{name: "no namespace", path: filepath.Join(root, hexKey[:2], hexKey), wantOK: true, wantKeyed: true},
		{name: "shard disagrees with key", path: filepath.Join(root, "zz", hexKey), wantOK: false},
		{name: "not a hex key", path: filepath.Join(root, "ab", "abcdef"), wantOK: false},
		{name: "key length but not hex", path: filepath.Join(root, "zz", strings.Repeat("z", hexKeyLength)), wantOK: false},
		{name: "uppercase hex key", path: filepath.Join(root, upperHex[:2], upperHex), wantOK: false},
		{name: "too deep", path: filepath.Join(root, "a", "b", hexKey[:2], hexKey), wantOK: false},
		{name: "bare file", path: filepath.Join(root, hexKey), wantOK: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			entry, ok := entryKeyForPath(root, test.path)
			assert.Equal(t, test.wantOK, ok)
			if !test.wantOK {
				return
			}
			assert.Equal(t, test.wantNS, entry.namespace)
			if test.wantKeyed {
				assert.Equal(t, key, entry.key)
			}
		})
	}
}
