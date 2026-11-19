#!/usr/bin/env bash
# bound: 90 s (three times its slowest green run, 30 s)
# The capability audit: every capability dev has names the PR that replaces it or the reason it
# is dropped, and the check itself catches a row that does neither.
set -euo pipefail
scripts/check-capability-audit.sh
scripts/check-capability-audit.sh --self-test
