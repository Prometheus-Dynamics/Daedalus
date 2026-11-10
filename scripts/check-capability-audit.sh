#!/usr/bin/env bash
# Checks testing/capability-audit.md: every row ends in PR ids that have briefs, or in
# `dropped: <reason>`, and every crate directory on dev is named in the file.
#
#   scripts/check-capability-audit.sh             check the audit
#   scripts/check-capability-audit.sh FILE        check another file, as the self-test does
#   scripts/check-capability-audit.sh --self-test check that bad rows are caught
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
readonly AUDIT=testing/capability-audit.md

# Prints every problem in the file named, and fails if there is one.
check() {
  local file=$1 status=0 line cell id number=0 rows=0
  while IFS= read -r line; do
    number=$((number + 1))
    # A table row that is not a header or a rule.
    [[ "$line" == \|* ]] || continue
    [[ "$line" =~ ^\|[-\ |]+\|$ ]] && continue
    cell="${line%|}"
    cell="${cell##*|}"
    cell="${cell#"${cell%%[![:space:]]*}"}"
    cell="${cell%"${cell##*[![:space:]]}"}"
    [[ "$cell" == "Replaced by" ]] && continue
    rows=$((rows + 1))
    if [[ "$cell" == dropped:* ]]; then
      if [[ -z "${cell#dropped:}" || "${cell#dropped:}" =~ ^[[:space:]]*$ ]]; then
        echo "$file:$number: dropped with no reason" >&2
        status=1
      fi
      continue
    fi
    # PR ids separated by commas, with an optional note in brackets after them.
    if [[ ! "$cell" =~ ^[A-Z][0-9]+(,\ [A-Z][0-9]+)*(\ \(.+\))?$ ]]; then
      echo "$file:$number: a row needs PR ids or 'dropped: <reason>', found '$cell'" >&2
      status=1
      continue
    fi
    for id in $(grep -oE '\b[A-Z][0-9]+\b' <<<"${cell%% (*}"); do
      if [[ ! -f "docs/prs/$id.md" ]]; then
        echo "$file:$number: $id has no brief in docs/prs/" >&2
        status=1
      fi
    done
  done <"$file"
  if ((rows == 0)); then
    echo "$file: no rows" >&2
    status=1
  fi

  # Every crate directory on dev is named. The directories are read from dev at the commits the
  # file records. A clone that does not have a commit tries to fetch it, and falls back to the
  # list the file itself records.
  local commit crates="" found
  while read -r commit; do
    if ! git cat-file -e "$commit^{commit}" 2>/dev/null; then
      git fetch -q --depth 1 origin "$commit" 2>/dev/null || true
    fi
    if found="$(git ls-tree -d --name-only "$commit" crates/ 2>/dev/null)"; then
      crates+="$found"$'\n'
    else
      echo "$file: dev commit $commit is not in this clone: using the recorded crate list for it" >&2
    fi
  done < <(sed -n 's/^- crates-at: //p' "$file")
  crates+="$(sed -n 's/^- crates: //p' "$file" | tr ' ' '\n' | sed '/^$/d; s|^|crates/|')"
  local crate
  while read -r crate; do
    [[ -n "$crate" ]] || continue
    if ! grep -qF "\`$crate\`" "$file"; then
      echo "$file: $crate is on dev and is named nowhere" >&2
      status=1
    fi
  done < <(sort -u <<<"$crates")
  return "$status"
}

# A planted bad row must fail the check, each kind of it.
self_test() {
  local work bad status=0
  work="$(mktemp -d)"
  trap 'rm -rf "$work"' RETURN
  local -a planted=(
    "| A planted capability | \`crates/core\` |  |"
    "| A planted capability | \`crates/core\` | soon |"
    "| A planted capability | \`crates/core\` | Z99 |"
    "| A planted capability | \`crates/core\` | dropped: |"
    "| A planted capability | \`crates/core\` | A4, later |"
  )
  for bad in "${planted[@]}"; do
    cp "$AUDIT" "$work/audit.md"
    printf '\n| Capability | On dev | Replaced by |\n|---|---|---|\n%s\n' "$bad" >>"$work/audit.md"
    if check "$work/audit.md" 2>/dev/null; then
      echo "self-test: a planted row passed: $bad" >&2
      status=1
    fi
  done
  # A crate that is named nowhere must fail too.
  sed 's/`crates\/wgsl-infer`/`crates\/elsewhere`/g' "$AUDIT" >"$work/audit.md"
  if check "$work/audit.md" 2>/dev/null; then
    echo "self-test: a crate named nowhere passed" >&2
    status=1
  fi
  return "$status"
}

case "${1:-}" in
  --self-test) self_test && echo "capability audit: the self-test caught every planted row" ;;
  "") check "$AUDIT" && echo "capability audit: every row has a PR or a reason, and every dev crate is named" ;;
  *) check "$1" ;;
esac
