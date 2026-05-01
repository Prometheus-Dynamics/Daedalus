#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

mapfile -t members < <(
    sed -n '/^members = \[/,/^\]/p' Cargo.toml \
        | sed -n 's/^[[:space:]]*"\([^"]*\)",\{0,1\}[[:space:]]*$/\1/p'
)

if ((${#members[@]} == 0)); then
    echo "no workspace members found in Cargo.toml" >&2
    exit 1
fi

violations=0

for member in "${members[@]}"; do
    manifest="$member/Cargo.toml"
    if [[ ! -f "$manifest" ]]; then
        echo "missing workspace manifest: $manifest" >&2
        violations=$((violations + 1))
        continue
    fi

    while IFS= read -r violation; do
        [[ -z "$violation" ]] && continue
        echo "$violation" >&2
        violations=$((violations + 1))
    done < <(
        awk -v manifest="$manifest" '
            function section_name(line, name) {
                name = line
                sub(/^\[/, "", name)
                sub(/\]$/, "", name)
                return name
            }

            function dependency_section(name) {
                return name == "dependencies" ||
                    name == "dev-dependencies" ||
                    name == "build-dependencies" ||
                    name ~ /^target\..*\.dependencies$/ ||
                    name ~ /^target\..*\.dev-dependencies$/ ||
                    name ~ /^target\..*\.build-dependencies$/
            }

            function allowed_exception(dep, line) {
                return manifest == "crates/engine/Cargo.toml" &&
                    (dep == "daedalus-runtime" ||
                     dep == "daedalus-planner" ||
                     dep == "daedalus-registry") &&
                    line ~ /path[[:space:]]*=/ &&
                    line ~ /default-features[[:space:]]*=[[:space:]]*false/
            }

            /^\[/ {
                section = section_name($0)
                active = dependency_section(section)
                next
            }

            active && /^[[:space:]]*[A-Za-z0-9_-]+[[:space:]]*=/ {
                line = $0
                sub(/[[:space:]]+#.*/, "", line)
                dep = line
                sub(/^[[:space:]]*/, "", dep)
                sub(/[[:space:]]*=.*/, "", dep)

                if (line ~ /workspace[[:space:]]*=[[:space:]]*true/) {
                    next
                }
                if (allowed_exception(dep, line)) {
                    next
                }

                print manifest ":" FNR ": dependency `" dep "` must use `workspace = true` or be documented in the dependency policy exceptions"
            }
        ' "$manifest"
    )
done

if ((violations > 0)); then
    echo "workspace dependency centralization check failed with $violations violation(s)" >&2
    exit 1
fi

echo "workspace dependency centralization check passed"
