#!/bin/sh
# Runs on the board, from the directory the bundle was unpacked in, and writes what it measures
# to results/. It is user space only: it starts the programs in bin/, a daemon of its own on a
# socket in this directory, and nothing else. It stops only what it started, by pid, changes
# nothing outside this directory, and needs no service stopped, no module and no permission.
#
#   measure-on-board.sh [batches]   batches of a thousand operations for each timing, 101
batches="${1:-101}"
here="$(cd "$(dirname "$0")" && pwd)"
out="$here/results"
mkdir -p "$out"
cd "$here" || exit 1
export LD_LIBRARY_PATH="$here/lib"

{
  uname -a
  cat /etc/os-release 2>/dev/null | grep -E '^(NAME|VERSION|PRETTY_NAME)='
  grep -E 'MemTotal|MemAvailable' /proc/meminfo
  getconf PAGESIZE 2>/dev/null
  cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null
} > "$out/board.txt" 2>&1

rss_kb() { # pid: what the process holds now, in kB
  sed -n 's/^VmRSS:[[:space:]]*\([0-9]*\).*/\1/p' "/proc/$1/status" 2>/dev/null
}

# A small host: time to its first step, and what it holds. Five times, since the first pays for
# pages nobody has read yet.
for n in 1 2 3 4 5; do ./bin/cpu-timings startup; done > "$out/startup.txt" 2>&1

# The per-step cost with the counters off and on, on a chain of sixteen steps, and the rest of
# the host's figures.
for section in probe_off probe_on host_round_trip host_idle_step clock_read; do
  ./bin/cpu-timings "$section" "$batches"
done > "$out/timings.txt" 2>&1

# Cycles and instructions for the same two runs, where the board has perf and lets it count.
if command -v perf >/dev/null 2>&1; then
  for section in probe_off probe_on; do
    echo "== $section"
    perf stat -e cycles,instructions ./bin/cpu-timings "$section" "$batches" 2>&1
  done > "$out/perf.txt" 2>&1
else
  echo "no perf on this board" > "$out/perf.txt"
fi

# The same sections against the reference revision, when the bundle has that program: the
# scorecard's other column, from the same board in the same run.
if [ -x ./bin/dev-measure ]; then
  {
    for n in 1 2 3 4 5; do ./bin/dev-measure startup; done
    for section in probe_off probe_on host_round_trip; do
      ./bin/dev-measure "$section" "$batches"
    done
    ./bin/dev-measure allocations
  } > "$out/dev.txt" 2>&1
  if command -v perf >/dev/null 2>&1; then
    for section in probe_off probe_on; do
      echo "== reference $section"
      perf stat -e cycles,instructions ./bin/dev-measure "$section" "$batches" 2>&1
    done >> "$out/perf.txt" 2>&1
  fi
fi

# A worker: a small value there and back, and one and four megabytes handed over.
{
  ./bin/process-timings ./bin/worker-fixture "$batches" 1
  ./bin/process-timings ./bin/worker-fixture 11 4
} > "$out/worker.txt" 2>&1

# The daemon: what it holds idle, with a plugin and a graph loaded, and after a run has stepped.
mkdir -p "$here/daemon/plugins"
cp "$here/lib/libdaedalus_dylib_fixture.so" "$here/daemon/plugins/fixture.so"
printf 'listen = ["unix:%s"]\nplugin_dir = "%s"\n' "$here/daemon/d.sock" "$here/daemon/plugins" \
  > "$here/daemon/daedalusd.toml"
started="$(date +%s%N 2>/dev/null)"
./bin/daedalusd --config "$here/daemon/daedalusd.toml" > "$out/daedalusd.log" 2>&1 &
daemon=$!
echo "$daemon" > "$here/daemon.pid"
ctl() { ./bin/daedalus-ctl --socket "$here/daemon/d.sock" "$@"; }
tries=0
until ctl ping >/dev/null 2>&1; do
  tries=$((tries + 1))
  if [ "$tries" -gt 500 ] || ! kill -0 "$daemon" 2>/dev/null; then break; fi
  sleep 0.01 2>/dev/null || sleep 1
done
answered="$(date +%s%N 2>/dev/null)"
{
  echo "daemon_start_to_first_answer_ns $((answered - started))"
  echo "daemon_rss_idle_kb $(rss_kb "$daemon")"
  ctl put-graph g "$here/g.json" >/dev/null 2>&1
  ctl plan g >/dev/null 2>&1
  echo "daemon_rss_graph_planned_kb $(rss_kb "$daemon")"
  ctl start g >/dev/null 2>&1
  ctl feed 1 in u32:5 >/dev/null 2>&1
  ctl step 1 >/dev/null 2>&1
  echo "daemon_rss_one_run_first_step_kb $(rss_kb "$daemon")"
  n=0
  while [ "$n" -lt 200 ]; do
    ctl feed 1 in u32:7 >/dev/null 2>&1
    ctl step 1 >/dev/null 2>&1
    ctl take 1 out >/dev/null 2>&1
    n=$((n + 1))
  done
  echo "daemon_rss_after_200_round_trips_kb $(rss_kb "$daemon")"
  echo "daemon_rss_peak_kb $(sed -n 's/^VmHWM:[[:space:]]*\([0-9]*\).*/\1/p' "/proc/$daemon/status")"
  echo "daemon_threads $(sed -n 's/^Threads:[[:space:]]*//p' "/proc/$daemon/status")"
} > "$out/daemon.txt" 2>&1
# The one process this stops is the daemon it started.
kill "$daemon" 2>/dev/null
wait "$daemon" 2>/dev/null
rm -f "$here/daemon.pid"
echo done > "$out/finished.txt"
