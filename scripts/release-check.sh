#!/usr/bin/env bash
# Whether a commit of next may be released, as far as the budget table goes:
#
#   scripts/release-check.sh [COMMIT]          (default HEAD)
#   scripts/release-check.sh --self-test
#
# The local gate leaves the budget table to the budgets workflow when its build directory never
# measured the change (`budgets=not-run(ci-only:release+callgrind+sizes)` on the commit's
# `Gate-Lanes:` line; testing/crates/budget-harness/check.py). If any commit since the last tag
# carries that, the release is refused unless the budgets workflow (.github/workflows/bench.yml),
# as it stands at the commit, installs valgrind and sets BUDGETS_REQUIRE_ALL=1, and its budgets
# job passed on the exact full commit. Red, cancelled, still running or never run: refused. A
# commit whose budgets workflow did not run (a docs-only push) is run by workflow_dispatch.
set -uo pipefail
# Run in the checkout whose history it reads.
SELF="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/release-check.sh"
CI_ONLY='budgets=not-run(ci-only:'

check() { # COMMIT: 0 accepted, 1 refused
  local sha range since runs workflow
  sha="$(git rev-parse --verify --quiet "$1^{commit}")" || { echo "release-check: no commit $1" >&2; return 1; }
  # The last release before it (a tag on the commit itself is the release being checked).
  since="$(git describe --tags --match 'v[0-9]*' --abbrev=0 "$sha^" 2>/dev/null)" &&
    range="$since..$sha" || range="$sha"
  # The landing commits keep the gate's record; the merges of next carry it in their second parent.
  # Read whole, then searched: `git log | grep -q` under pipefail fails when grep's early exit
  # kills git log, which would read as no such commit.
  local messages
  messages="$(git log --format=%B "$range")" || { echo "release-check: $sha: cannot read $range" >&2; return 1; }
  if ! grep -qF "$CI_ONLY" <<< "$messages"; then
    echo "release-check: $sha: no gate since ${since:-the start} left the budget table to CI"
    return 0
  fi
  workflow="$(git show "$sha:.github/workflows/bench.yml" 2>/dev/null)"
  if ! grep -qE 'apt-install\.sh .*valgrind' <<< "$workflow" ||
     ! grep -qE 'BUDGETS_REQUIRE_ALL: "1"' <<< "$workflow"; then
    echo "release-check: $sha: refused: the budgets workflow there does not install valgrind and" \
      "set BUDGETS_REQUIRE_ALL=1, so its pass would not cover the rows the local gate left to it" >&2
    return 1
  fi
  runs="$("${GH:-gh}" run list --workflow bench.yml --commit "$sha" --limit 20 \
    --json databaseId,headSha,status,conclusion 2>&1)" || {
    echo "release-check: $sha: refused: the budgets workflow's runs could not be read: $runs" >&2
    return 1
  }
  local id
  for id in $(python3 -I -c '
import json, sys
for run in json.loads(sys.argv[1]):
    if run["headSha"] == sys.argv[2] and run["status"] == "completed" and run["conclusion"] == "success":
        print(run["databaseId"])' "$runs" "$sha"); do
    # The run passed; its budgets job is the one that measures the rows.
    if [[ "$("${GH:-gh}" run view "$id" --json jobs \
          --jq '[.jobs[] | select(.name == "budgets") | .conclusion] | join(",")' 2>/dev/null)" == success ]]; then
      echo "release-check: $sha: the budget rows the local gate left to CI passed there (run $id)"
      return 0
    fi
  done
  echo "release-check: $sha: refused: a commit since ${since:-the start} left the budget table to" \
    "CI, and the budgets workflow has no passing budgets job on $sha (red, missing or not done):" \
    "run it (gh workflow run bench.yml --ref next) and check again" >&2
  return 1
}

self_test() {
  # A planted repository with the real workflow file, and a gh that answers from planted runs.
  local work status=0
  work="$(mktemp -d)"
  trap 'rm -rf "$work"' RETURN
  local real; real="$(cat "$(dirname "$SELF")/../.github/workflows/bench.yml")"
  (
    cd "$work" && git init -q -b next && git config user.email t@t && git config user.name t &&
    mkdir -p .github/workflows && printf '%s\n' "$real" > .github/workflows/bench.yml &&
    git add -A && git commit -qm base && git tag v0.0.1 &&
    echo 1 > a && git add a && git commit -qm "measured
Gate-Lanes: host=passed beside=passed" && git tag measured &&
    echo 2 > a && git commit -qam "left to CI
Gate-Lanes: host=passed beside=passed budgets=not-run(ci-only:release+callgrind+sizes)" &&
    echo 3 > a && git commit -qam "docs after it
Gate-Lanes: host=not-run(docs only)" && git tag later
  ) || return 1
  mkdir -p "$work/bin"
  cat > "$work/bin/gh" <<'EOF'
#!/usr/bin/env bash
# run list ... --commit SHA ...: the planted runs; run view ID --json jobs ...: the planted job.
if [[ $1 == run && $2 == list ]]; then
  sha=""; prev=""; for a in "$@"; do [[ $prev == --commit ]] && sha=$a; prev=$a; done
  case "$PLANT" in
    missing) echo '[]' ;;
    other) echo "[{\"databaseId\":7,\"headSha\":\"0000\",\"status\":\"completed\",\"conclusion\":\"success\"}]" ;;
    red) echo "[{\"databaseId\":7,\"headSha\":\"$sha\",\"status\":\"completed\",\"conclusion\":\"failure\"}]" ;;
    running) echo "[{\"databaseId\":7,\"headSha\":\"$sha\",\"status\":\"in_progress\",\"conclusion\":\"\"}]" ;;
    *) echo "[{\"databaseId\":7,\"headSha\":\"$sha\",\"status\":\"completed\",\"conclusion\":\"success\"}]" ;;
  esac
elif [[ $1 == run && $2 == view ]]; then
  [[ $PLANT == job-red ]] && echo failure || echo success
else
  exit 2
fi
EOF
  chmod +x "$work/bin/gh"
  local spec want plant commit label got
  for spec in "accept|green|later|CI green on the exact commit" \
              "refuse|red|later|CI red" \
              "refuse|missing|later|CI missing" \
              "refuse|running|later|CI still running" \
              "refuse|other|later|CI green on another commit only" \
              "refuse|job-red|later|the run green but its budgets job red" \
              "refuse|nobudgetsenv|later|a workflow without BUDGETS_REQUIRE_ALL=1" \
              "accept|missing|measured|nothing left to CI, no CI needed"; do
    IFS='|' read -r want plant commit label <<< "$spec"
    if [[ $plant == nobudgetsenv ]]; then
      (cd "$work" && git checkout -q later && sed -i '/BUDGETS_REQUIRE_ALL/d' .github/workflows/bench.yml &&
        git commit -qam "drop it" && git tag -f later >/dev/null) || return 1
      plant=green
    fi
    got=accept
    (cd "$work" && PLANT=$plant GH="$work/bin/gh" bash "$SELF" "$commit" >/dev/null 2>&1) ||
      got=refuse
    if [[ $got == "$want" ]]; then
      echo "release-check self-test: $label: $want"
    else
      echo "release-check self-test: $label: $got, not $want" >&2
      status=1
    fi
  done
  return $status
}

if [[ ${1:-} == --self-test ]]; then
  self_test
  exit
fi
check "${1:-HEAD}"
