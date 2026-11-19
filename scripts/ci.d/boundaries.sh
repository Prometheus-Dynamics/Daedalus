#!/usr/bin/env bash
# bound: 1317 s (three times its slowest green run, 439 s, in a cold build on a fresh slot)
set -euo pipefail
scripts/check-boundaries.sh
# The board-run judges are checked against planted runs, so a judge that can't fail is caught.
python3 scripts/board/judge.py --self-test
