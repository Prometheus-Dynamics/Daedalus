#!/usr/bin/env bash
# The package surface: every crate that can be published lists its files with no warning, has
# the metadata a registry asks for, and depends on nothing that can't be published. Examples,
# fixtures and the test crates are `publish = false` and aren't looked at.
set -euo pipefail

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

failed=0
for crate in "${crates[@]}"; do
  # --allow-dirty: the list is of what is on disk, which in a PR isn't committed yet.
  if ! output=$(cargo package --locked --list --allow-dirty -p "$crate" 2>&1 >/dev/null); then
    echo "package: $crate: cargo package --list failed" >&2
    echo "$output" >&2
    failed=1
  elif grep -qi 'warning' <<<"$output"; then
    echo "package: $crate: cargo package --list warned" >&2
    echo "$output" >&2
    failed=1
  fi
done
[[ $failed == 0 ]] || exit 1
echo "  ${#crates[@]} crates list their files with no warning"
