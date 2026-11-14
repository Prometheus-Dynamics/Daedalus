#!/bin/sh
# Runs on the board, from the directory the bundle was unpacked in. It runs each test binary in
# bin/ once, one at a time, and leaves <name>.out and <name>.exit in results/. It uses nothing a
# small busybox lacks, changes nothing outside its own directory, and kills only what it started.
#
#   on-board.sh [seconds]   the longest one binary may run, 300 unless given
limit="${1:-300}"
here="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$here/results"
uname -a > "$here/results/uname.txt" 2>&1
for bin in "$here"/bin/*; do
  name="$(basename "$bin")"
  "$bin" --test-threads=1 > "$here/results/$name.out" 2>&1 &
  pid=$!
  echo "$pid" > "$here/run.pid"
  # The one process this may ever kill is the one it just started, by its own pid.
  ( sleep "$limit"; kill "$pid" 2>/dev/null ) &
  watch=$!
  wait "$pid"
  echo "$?" > "$here/results/$name.exit"
  kill "$watch" 2>/dev/null
  wait "$watch" 2>/dev/null
  rm -f "$here/run.pid"
done
