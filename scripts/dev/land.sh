#!/usr/bin/env bash
# Lands one PR into next: gates the change, commits it, opens the PR and merges it.
#
#   scripts/dev/land.sh ID TITLE < BODY
#
# Run inside a clone of next (wt/gate, scripts/dev/setup.sh), on branch pr/ID, with the change
# uncommitted and scripts/dev/env.sh sourced. Runs nothing whose result is already known:
#   - TODO.md and AGENTS.md alone: no check reads them, so no gate;
#   - those and docs/*.md alone: the steps that read docs/ (doc, boundaries, and daedalus's docs::
#     tests);
#   - anything else: the whole gate.
# The gate's summary (its wall time and each step's) goes in the commit message, the record of
# what was run.
set -uo pipefail
id="$1"; title="$2"; body="$(cat)"
root="$(git rev-parse --show-toplevel)" && cd "$root" || exit 1
[ "$(git branch --show-current)" = "pr/$id" ] || { echo "WRONG BRANCH"; exit 1; }
: "${CARGO_TARGET_DIR:?source scripts/dev/env.sh}" "${TMPDIR:?}" "${LAND_CPUS:?}" "${DAEDALUS_LOGS:?}"
log="$DAEDALUS_LOGS/gate-$id.log"
timeout 90 git fetch -q origin next || { echo "FETCH FAILED"; exit 1; }
git merge -q --ff-only origin/next || { echo "NEXT MOVED AND OVERLAPS: merge origin/next and resolve"; exit 1; }
changed="$(git status --porcelain=v1 -uall | cut -c4- | sed 's/.* -> //')"
[ -n "$changed" ] || { echo "NOTHING TO LAND"; exit 1; }
if ! command grep -qvE '^(TODO\.md|AGENTS\.md)$' <<<"$changed"; then
  gate=(true); kind="notes only: no gate"
elif ! command grep -qvE '^(TODO\.md|AGENTS\.md|docs/.*\.md)$' <<<"$changed"; then
  gate=(bash -c 'scripts/ci.sh doc boundaries && cargo test --locked -q -p daedalus --test all -- docs::')
  kind="docs only: doc, boundaries, docs:: tests"
else
  gate=(scripts/gate.sh); kind="whole gate"
fi
echo "$kind"
start=$SECONDS
timeout "${LAND_GATE_TIMEOUT:-1600}" taskset -c "$LAND_CPUS" ionice -c3 nice -n10 "${gate[@]}" > "$log" 2>&1; g=$?
wall=$((SECONDS - start))
if [ $g != 0 ]; then
  echo "GATE FAILED ($g), log $log"
  command grep -nE '^error|^warning: unused|FAILED|panicked|^[a-z-]+: [a-z/].*:[0-9]+:|Diff in' "$log" | head -40 | cut -c1-200
  exit 1
fi
summary="$(printf 'Gate (%s): %s s, load %s.\n' "$kind" "$wall" "$(cut -d' ' -f1-3 /proc/loadavg)"; command grep -E '^    [a-z0-9.-]+: [0-9]+ s$' "$log" | sed 's/^ */  /')"
echo "$summary"
git add -A && git commit -q -m "$id: $title

$(printf '%s' "$body" | sed '/^🤖/d')

$summary

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>" || exit 1
timeout 90 git push -q -u origin "pr/$id" 2>/dev/null || { echo "PUSH FAILED"; exit 1; }
url=$(timeout 60 gh pr create --base next --head "pr/$id" --title "$id: $title" --body "$body

$summary

🤖 Generated with [Claude Code](https://claude.com/claude-code)") || { echo "PR CREATE FAILED"; exit 1; }
echo "$url"
for try in 1 2 3; do timeout 60 gh pr merge "$url" --merge >/dev/null 2>&1 && break; [ $try = 3 ] && { echo "MERGE FAILED"; exit 1; }; sleep 5; done
timeout 60 git push -q origin --delete "pr/$id" 2>/dev/null
timeout 90 git fetch -q origin next; git checkout -q --detach origin/next && git branch -q -D "pr/$id"; git rev-parse --short HEAD
