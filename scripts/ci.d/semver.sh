#!/usr/bin/env bash
# Semantic versioning against the last release: cargo semver-checks from the tag named in the
# file SEMVER_BASELINE at the repository's root. It reports and never fails the gate. Until a
# release is called there is no baseline, and this does nothing: the owner writes the file.
set -uo pipefail

if [[ ! -s SEMVER_BASELINE ]]; then
  echo "  no SEMVER_BASELINE: nothing has been released to compare with"
  exit 0
fi
baseline=$(tr -d '[:space:]' < SEMVER_BASELINE)
if ! command -v cargo-semver-checks >/dev/null; then
  echo "  cargo-semver-checks isn't installed: not checked against $baseline"
  exit 0
fi
if ! git rev-parse -q --verify "$baseline^{commit}" >/dev/null; then
  echo "  the baseline \`$baseline\` isn't a commit in this clone: not checked"
  exit 0
fi
if cargo semver-checks --workspace --baseline-rev "$baseline"; then
  echo "  no breaking change since $baseline"
else
  echo "  semver: breaking changes since $baseline are listed above. This step doesn't fail the gate."
fi
exit 0
