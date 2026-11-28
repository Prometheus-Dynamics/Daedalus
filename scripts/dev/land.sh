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
# The gate's summary (its wall time, each step's, and the `Gate-Lanes:` line saying what each lane
# did: passed, passed(K-of-N-reused), or not-run(WHY)) goes in the commit message, the record of
# what was run.
set -uo pipefail
id="$1"; title="$2"; body="$(cat)"
root="$(git rev-parse --show-toplevel)" && cd "$root" || exit 1
[ "$(git branch --show-current)" = "pr/$id" ] || { echo "WRONG BRANCH"; exit 1; }
: "${CARGO_TARGET_DIR:?source scripts/dev/env.sh}" "${DAEDALUS_CARGO_HOME:?}" "${TMPDIR:?}" "${LAND_CPUS:?}" "${DAEDALUS_LOGS:?}" "${NICE:?}"
log="$DAEDALUS_LOGS/gate-$id.log"
timeout 90 git fetch -q origin next || { echo "FETCH FAILED"; exit 1; }
git merge -q --ff-only origin/next || { echo "NEXT MOVED AND OVERLAPS: merge origin/next and resolve"; exit 1; }
changed="$(git status --porcelain=v1 -uall | cut -c4- | sed 's/.* -> //')"
[ -n "$changed" ] || { echo "NOTHING TO LAND"; exit 1; }
all_lanes="host docs chips ship beside cross scripts"
not_run() { local lane out="Gate-Lanes:"; for lane in $all_lanes; do out+=" $lane=${1//LANE/$lane}"; done; echo "$out"; }
if ! command grep -qvE '^(TODO\.md|AGENTS\.md)$' <<<"$changed"; then
  gate=(true); kind="notes only: no gate"
  lanes_line="$(not_run 'not-run(notes only)')"
elif ! command grep -qvE '^(TODO\.md|AGENTS\.md|docs/.*\.md)$' <<<"$changed"; then
  gate=(bash -c 'scripts/ci.sh doc boundaries && cargo test --locked -q -p daedalus --test all -- docs::')
  kind="docs only: doc, boundaries, docs:: tests"
  lanes_line="Gate-Lanes: host=not-run(docs only: the docs:: tests ran) docs=passed chips=not-run(docs only) ship=not-run(docs only) beside=not-run(docs only) cross=not-run(docs only: boundaries ran) scripts=not-run(docs only)"
else
  gate=(scripts/gate.sh); kind="whole gate"
fi
echo "$kind"
# The gate runs offline in its own cargo homes, one per lane, fetched first (scripts/dev/cargo-home.sh).
if [ "${gate[0]}" != true ]; then
  fetched="$DAEDALUS_LOGS/fetch-$id.log"
  scripts/dev/cargo-home.sh $all_lanes > "$fetched" 2>&1 || { echo "FETCH FAILED, log $fetched"; tail -5 "$fetched"; exit 1; }
  command grep -E '^fetch: ([0-9]+ registry|sources)' "$fetched" | cut -c1-200
  export CARGO_HOME="$DAEDALUS_CARGO_HOME" GATE_LANE_HOMES="$DAEDALUS_CARGO_HOME.lanes"
fi
start=$SECONDS
timeout "${LAND_GATE_TIMEOUT:-1600}" taskset -c "$LAND_CPUS" ionice -c3 nice -n"$NICE" "${gate[@]}" > "$log" 2>&1; g=$?
wall=$((SECONDS - start))
if [ $g != 0 ]; then
  echo "GATE FAILED ($g), log $log"
  command grep -nE '^error|^warning: unused|FAILED|panicked|^[a-z-]+: [a-z/].*:[0-9]+:|Diff in' "$log" | head -40 | cut -c1-200
  exit 1
fi
[ "${gate[0]}" != scripts/gate.sh ] || lanes_line="$(command grep -E '^Gate-Lanes: ' "$log" | tail -1)"
[ -n "${lanes_line:-}" ] || { echo "THE GATE PASSED WITHOUT A Gate-Lanes LINE, log $log"; exit 1; }
cpu="$(command sed -nE 's/^gate: [0-9]+ s, ([0-9]+) cpu-s$/, \1 cpu-s/p' "$log" | tail -1)"
summary="$(printf 'Gate (%s): %s s%s, load %s, nice %s.\n' "$kind" "$wall" "$cpu" "$(cut -d' ' -f1-3 /proc/loadavg)" "$NICE"; command grep -E '^    [a-z0-9.-]+: [0-9]+ s, [0-9]+ cpu-s$' "$log" | sed 's/^ */  /'; printf '\n%s\n' "$lanes_line")"
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
