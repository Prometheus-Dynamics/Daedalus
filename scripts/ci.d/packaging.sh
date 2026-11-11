#!/usr/bin/env bash
# Checks the daemon's packaging files against the daemon: both units verify, the sample config
# is accepted, and the paths agree. Nothing is installed or started. The check's planted
# breakages must each fail (its --self-test), so the check is shown to catch what it claims.
set -euo pipefail
service/daedalusd/packaging/check.sh
service/daedalusd/packaging/check.sh --self-test
