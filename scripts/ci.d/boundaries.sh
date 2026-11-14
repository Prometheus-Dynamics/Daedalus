#!/usr/bin/env bash
set -euo pipefail
scripts/check-boundaries.sh
# The board-run judges are checked against planted runs, so a judge that can't fail is caught.
python3 scripts/board/judge.py --self-test
