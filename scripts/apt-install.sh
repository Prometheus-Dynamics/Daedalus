#!/usr/bin/env bash
# Installs Ubuntu packages on a CI runner, retrying while the mirrors can't be reached.
#
# A runner whose DNS or mirror is briefly down fails `apt-get update` before any of the
# repository's code runs, and that failure says nothing about the code. So the install is
# retried with a backoff that grows to 10 seconds and stays there, for up to 15 minutes, and
# only then does the step fail.
#
# Usage: scripts/apt-install.sh <package>...
set -euo pipefail

if [ "$#" -eq 0 ]; then
    echo "apt-install: name the packages to install" >&2
    exit 2
fi

deadline=$((SECONDS + 900))
delay=1
attempt=1
while true; do
    # `apt-get update` only warns when an index can't be fetched, so the install is what shows
    # whether the packages could be reached.
    if sudo apt-get update && sudo apt-get install -y "$@"; then
        exit 0
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
        echo "apt-install: the packages couldn't be installed after $attempt attempts in 15 minutes" >&2
        exit 1
    fi
    echo "apt-install: attempt $attempt failed, retrying in ${delay}s" >&2
    sleep "$delay"
    attempt=$((attempt + 1))
    delay=$((delay * 2))
    if [ "$delay" -gt 10 ]; then
        delay=10
    fi
done
