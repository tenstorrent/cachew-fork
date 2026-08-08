package gomod

import (
	"context"
	"io"
	"io/fs"
	"strings"

	"github.com/alecthomas/errors"

	"github.com/tenstorrent/internal-cachew/internal/cache"
	"github.com/tenstorrent/internal-cachew/internal/gomodobject"
)

type goproxyCacher struct {
	cache cache.Cache
	// private reports whether a module proxy path resolves to a private
	// repository. It is recorded with every object so that reads of it through
	// the generic object API can be authorized; a nil predicate marks everything
	// public, which is only correct where no private paths are configured.
	private func(name string) bool
}

func (g *goproxyCacher) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	key := cache.NewKey(name)

	rc, _, err := g.cache.Open(ctx, key)
	if err != nil {
		return nil, fs.ErrNotExist
	}

	return rc, nil
}

func (g *goproxyCacher) Put(ctx context.Context, name string, content io.ReadSeeker) error {
	if strings.HasSuffix(name, "/@v/list") || strings.HasSuffix(name, "/@latest") {
		return nil
	}

	object := gomodobject.New(name, g.private != nil && g.private(name))

	wc, err := g.cache.Create(ctx, object.Key, object.Headers(), 0)
	if err != nil {
		return errors.Errorf("create cache entry: %w", err)
	}

	if _, err := content.Seek(0, io.SeekStart); err != nil {
		return errors.Join(errors.Errorf("seek to start: %w", err), wc.Abort(err))
	}

	if _, err := io.Copy(wc, content); err != nil {
		return errors.Join(errors.Errorf("write to cache: %w", err), wc.Abort(err))
	}

	if err := wc.Close(); err != nil {
		return errors.Errorf("close cache entry: %w", err)
	}

	return nil
}
