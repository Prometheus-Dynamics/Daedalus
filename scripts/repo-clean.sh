#!/usr/bin/env bash
# The tree has no file that a build made and git doesn't ignore. Run after a build and the
# tests, in a clean checkout: anything untracked there is something that was generated.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

untracked=$(git status --porcelain --untracked-files=all | grep '^??' || true)
if [[ -n "$untracked" ]]; then
  echo "repo-clean: files that aren't tracked and aren't ignored:" >&2
  echo "$untracked" >&2
  exit 1
fi
changed=$(git status --porcelain --untracked-files=no)
if [[ -n "$changed" ]]; then
  echo "repo-clean: tracked files that a build changed:" >&2
  echo "$changed" >&2
  exit 1
fi
echo "repo-clean: nothing generated is left in the tree"
