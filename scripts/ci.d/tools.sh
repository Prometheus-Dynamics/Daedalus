#!/usr/bin/env bash
# The working tools check themselves against planted cases: which packages a change reaches
# (scripts/check.sh runs those), and which build directories may be deleted.
set -euo pipefail
python3 -I scripts/affected.py --self-test
python3 -I scripts/prune-targets.py --self-test
# A build directory another checkout built in is refused before anything builds.
planted="$(mktemp -d)"
trap 'rm -rf "$planted"' EXIT
echo /a/checkout/elsewhere > "$planted/.daedalus-checkout"
status=0
refused="$(CARGO_TARGET_DIR="$planted" scripts/ci.sh format 2>&1)" || status=$?
if [[ $status != 0 ]] && grep -q "belongs to the checkout /a/checkout/elsewhere" <<< "$refused" &&
  ! grep -q "^==> " <<< "$refused"; then
  echo "tools: a build directory of another checkout is refused"
else
  echo "tools: a build directory of another checkout was used" >&2
  exit 1
fi
