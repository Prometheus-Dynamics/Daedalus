#!/usr/bin/env bash
# Checks the daemon's packaging files against the daemon: both units verify, the sample config
# is accepted, and the paths agree. Nothing is installed or started.
set -euo pipefail
exec service/daedalusd/packaging/check.sh
