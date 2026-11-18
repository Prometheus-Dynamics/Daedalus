#!/usr/bin/env bash
# The gate's checks for what a change can reach: a quick answer while working, never instead of
# the gate.
#
#   scripts/check.sh [BASE]     the change since this branch left BASE (default origin/next),
#                               committed or not
#
# Every check here is one the gate runs, run the same way: format and clippy over the workspace,
# the file-size rule, and the gate's own test build, from which only some binaries run: those of
# the packages the change can reach (scripts/affected.py), and `daedalus`, whose tests read the
# whole tree. A change outside every package runs every binary. So a red here is a red at the
# gate; a green here is not a green there, which runs the other steps and every binary.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

reached="$(scripts/affected.py "$@")"
if [[ "$reached" == all ]]; then
  echo "check: a file outside every package changed: every test binary runs"
  scripts/ci.sh format clippy test
else
  packages="$(printf '%s\n' $reached daedalus | sort -u | tr '\n' ' ')"
  echo "check: test binaries of $(wc -w <<< "$packages") packages: $packages"
  DAEDALUS_TEST_PACKAGES="$packages" scripts/ci.sh format clippy test
fi
scripts/check-file-sizes.sh
echo "check: passed (the gate runs the rest)"
