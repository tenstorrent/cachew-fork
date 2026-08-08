#!/usr/bin/env bash
set -euo pipefail

# Syncs this fork's trunk with upstream block/cachew.
#
# This repo's module is deliberately named github.com/tenstorrent/internal-cachew,
# decoupled from upstream's github.com/block/cachew, so that this fork can be
# consumed as a real Go dependency by other Tenstorrent modules without a
# module-path/fetch-path mismatch. Upstream itself is mirrored unmodified as
# this repo's own `main` branch (module path untouched, so it stays a clean
# mirror), and this fork's trunk (tt-main) carries the renamed import paths on
# top of it.
#
# Merging `main` into `tt-main` is therefore a normal 3-way merge for every
# file upstream didn't touch (git sees only "ours" changed the import lines,
# so those survive cleanly with zero conflicts). It only produces real merge
# conflicts when upstream edits the same import lines this fork renamed, which
# is rare. What it does NOT do automatically is fix up brand-new files that
# upstream added since the last sync -- those arrive still importing
# github.com/block/cachew, since there's no "ours" version to 3-way-merge
# against. This script's second phase catches that: after the merge, it greps
# for any remaining references to the upstream module path and rewrites them.
#
# Usage:
#   scripts/sync-upstream.sh
#
# Run from a clean working tree with the sync target branch checked out
# (normally tt-main). If the merge step produces conflicts, resolve them
# normally and `git commit`, then re-run this script -- the merge step is a
# no-op once `main` is already merged, so it will proceed straight to the
# normalization pass.

OLD_PATH="github.com/block/cachew"
NEW_PATH="github.com/tenstorrent/internal-cachew"

if [[ -n "$(git status --porcelain)" ]]; then
  echo "error: working tree is not clean" >&2
  exit 1
fi

echo "Fetching origin/main (upstream mirror)..."
git fetch origin main

echo "Merging origin/main..."
if ! git merge --no-edit origin/main; then
  cat >&2 <<EOF

Merge produced conflicts. Resolve them normally -- most will be import lines
where upstream touched the same region this fork renamed. Once resolved:

  git add <files>
  git commit

Then re-run this script to finish the normalization pass.
EOF
  exit 1
fi

echo "Normalizing any reintroduced upstream import paths..."
mapfile -t files < <(git grep -l "$OLD_PATH" -- '*.go' '.github/workflows/*.yml' 2>/dev/null || true)
if ((${#files[@]} > 0)); then
  sed -i "s#${OLD_PATH}#${NEW_PATH}#g" "${files[@]}"
  git add "${files[@]}"
  git commit -m "chore: normalize imports from upstream sync to ${NEW_PATH}"
  echo "Normalized ${#files[@]} file(s)."
else
  echo "Nothing to normalize -- no upstream import paths reintroduced."
fi

echo "Validating..."
export PATH="/workspace/group/tools/go/bin:$PATH"
gofmt -l . | grep -v '^rust/' || true
go build ./...
go vet ./...
if ! go test ./...; then
  echo
  echo "warning: some tests failed. internal/cache and internal/metadatadb/s3" >&2
  echo "failing solely on a missing docker binary is expected in a container-" >&2
  echo "less sandbox. Any other failure is a real regression -- do not push." >&2
fi

echo
echo "Sync complete. Review the result, then push manually:"
echo "  git push origin HEAD:tt-main"
