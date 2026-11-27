#!/usr/bin/env bash
# bound: 264 s (three times its slowest green run, 88 s)
# The package surface: every crate that can be published lists its files with no warning, has
# the metadata a registry asks for, and depends on nothing that can't be published. Examples,
# fixtures and the test crates are `publish = false` and aren't looked at.
set -euo pipefail

# Lists each crate's files: one cargo run for them all, the package cache's lock taken once
# (one run a crate took the lock 43 times, 16-26 s of the step in gates, against 0.77 s). On a
# failure or a warning the crates are listed one by one, to name each one that failed or warned.
# `list` takes the workspace directory, then the crates.
list() {
  local dir=$1 crate output failed=0 named=0 batch=()
  shift
  for crate in "$@"; do batch+=(-p "$crate"); done
  # --allow-dirty: the list is of what is on disk, which in a PR isn't committed yet.
  if output=$(cd "$dir" && cargo package --locked --list --allow-dirty "${batch[@]}" 2>&1 >/dev/null) &&
    ! grep -qi 'warning' <<<"$output"; then
    return 0
  fi
  for crate in "$@"; do
    if ! output=$(cd "$dir" && cargo package --locked --list --allow-dirty -p "$crate" 2>&1 >/dev/null); then
      echo "package: $crate: cargo package --list failed" >&2
      echo "$output" >&2
      named=1
    elif grep -qi 'warning' <<<"$output"; then
      echo "package: $crate: cargo package --list warned" >&2
      echo "$output" >&2
      named=1
    fi
  done
  # Failed or warned together and not one by one: still a failure, with what it said.
  [[ $named == 1 ]] || { echo "package: listing the crates together failed or warned:" >&2; echo "$output" >&2; }
  return 1
}

if [[ "${1:-}" == --self-test ]]; then
  # A planted workspace: a clean crate passes; a crate that warns, and one whose listing fails,
  # fail the listing, each named, the clean one not.
  plant=$(mktemp -d)
  trap 'rm -rf "$plant"' EXIT
  status=0
  crate() {
    mkdir -p "$plant/$1/src"
    : > "$plant/$1/src/lib.rs"
    printf '[package]\nname = "%s"\nversion = "0.1.0"\nedition = "2021"\nlicense = "MIT"\ndescription = "planted"\n%s\n' "$1" "$2" > "$plant/$1/Cargo.toml"
  }
  check() {
    local case=$1 want=$2 named=$3 got=0 said
    shift 3
    printf '[workspace]\nmembers = [%s]\nresolver = "2"\n' "$(printf '"%s",' "$@")" > "$plant/Cargo.toml"
    (cd "$plant" && cargo generate-lockfile --offline -q)
    said=$(list "$plant" "$@" 2>&1) || got=1
    if [[ $got != "$want" ]] || { [[ -n $named ]] && ! grep -q "package: $named: " <<<"$said"; } ||
      grep -q "package: clean: " <<<"$said"; then
      echo "package self-test: $case: exit $got (want $want), said: $said" >&2
      status=1
    fi
  }
  repository='repository = "https://example.invalid/planted"'
  crate clean "$repository"
  # No repository, documentation or homepage: cargo warns.
  crate warns ""
  crate fails "$repository"$'\n''readme = "MISSING.md"'
  check "a clean crate" 0 "" clean
  check "a crate that warns" 1 warns clean warns
  check "a crate whose listing fails" 1 fails clean fails
  [[ $status == 0 ]] && echo "package self-test: a clean crate lists; one that warns and one that fails fail the listing, each named, the clean one not"
  exit $status
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cargo metadata --locked --no-deps --format-version 1 > "$work/metadata.json"
# The names of the crates that can be published, after checking each one's manifest.
python3 - "$work/metadata.json" > "$work/crates" <<'PY'
import json, sys
meta = json.load(open(sys.argv[1]))
private = {p["name"] for p in meta["packages"] if p.get("publish") == []}
status = 0
for package in meta["packages"]:
    if package["name"] in private:
        continue
    for field in ("description", "license", "repository"):
        if not package.get(field):
            print(f"package: {package['name']} has no `{field}`", file=sys.stderr)
            status = 1
    for dep in package["dependencies"]:
        # A dev-dependency isn't part of what is published.
        if dep.get("kind") != "dev" and dep["name"] in private:
            print(f"package: {package['name']} depends on {dep['name']}, which can't be published",
                  file=sys.stderr)
            status = 1
    print(package["name"])
sys.exit(status)
PY
mapfile -t crates < "$work/crates"
if [[ ${#crates[@]} == 0 ]]; then
  echo "package: no crate can be published" >&2
  exit 1
fi

list . "${crates[@]}" || exit 1
echo "  ${#crates[@]} crates list their files with no warning"
