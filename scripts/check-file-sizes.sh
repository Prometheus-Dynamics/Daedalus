#!/usr/bin/env bash
# Rule file-size: no tracked Rust file over 800 lines. Usage: check-file-sizes.sh [--root DIR]
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ "${1:-}" == "--root" ]]; then root="$2"; fi
cd "$root"
status=0
while IFS= read -r -d '' file; do
  [[ "$file" == *.rs && -f "$file" ]] || continue
  lines=$(wc -l < "$file")
  if (( lines > 800 )); then
    echo "file-size: $file:$lines: over 800 lines"
    status=1
  fi
done < <(git ls-files -z --cached --others --exclude-standard . 2>/dev/null || find . -name '*.rs' -print0)
exit "$status"
