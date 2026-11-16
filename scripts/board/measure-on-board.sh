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
# Two daemons are measured at the same points, into two files with the same names: the full one
# plans its graph, and the lean one (built without the planner) loads the plan the bundle carries,
# which the full one made from the same graph.
# Nanoseconds now. A busybox `date` prints `%N` as it is, so a board without a finer clock in
# `date` reads /proc/uptime, whose resolution is 10 ms, and the figure says which it was.
now_ns() {
  t="$(date +%s%N 2>/dev/null)"
  case "$t" in
    '' | *[!0-9]*)
      read -r up _ < /proc/uptime
      hundredths="${up#*.}"
      echo $(( ${up%.*} * 1000000000 + ${hundredths#0} * 10000000 ))
      ;;
    *) echo "$t" ;;
  esac
}
case "$(date +%s%N 2>/dev/null)" in
  '' | *[!0-9]*) clock_resolution_ns=10000000 ;;
  *) clock_resolution_ns=1 ;;
esac

ctl() { ./bin/daedalus-ctl --socket "$socket" "$@"; }

# Waits until the daemon in $daemon answers on $socket, and stops when it has gone.
wait_for_daemon() {
  tries=0
  until ctl ping >/dev/null 2>&1; do
    tries=$((tries + 1))
    if [ "$tries" -gt 500 ] || ! kill -0 "$daemon" 2>/dev/null; then break; fi
    sleep 0.01 2>/dev/null || sleep 1
  done
}

# Starts a daemon with the config in $1 and its log in $2, on the socket in $3, and sets daemon,
# socket, started and answered.
start_daemon() {
  started="$(now_ns)"
  ./bin/"$4" --config "$1" > "$2" 2>&1 &
  daemon=$!
  echo "$daemon" > "$here/daemon.pid"
  socket="$3"
  wait_for_daemon
  answered="$(now_ns)"
}

stop_daemon() {
  kill "$daemon" 2>/dev/null
  wait "$daemon" 2>/dev/null
  rm -f "$here/daemon.pid"
}

# The workers daemon's figures, taken at the points the daemon's are: its own resident memory, and
# the resident memory of its worker, the one child it has. $1 is the file to write.
workers_figures() {
  local i w
  worker_rss() { # the worker's resident memory now, in kB, or nothing when it isn't running
    # A worker is a child of the thread that started it, which need not be the main thread: read
    # every thread's list.
    w="$(cat /proc/$daemon/task/*/children 2>/dev/null | awk '{print $1}' | head -n 1)"
    [ -n "$w" ] && rss_kb "$w"
  }
  # The worker starts with the daemon: wait for it, so the idle figure is taken once it is up.
  i=0
  while [ -z "$(worker_rss)" ] && [ "$i" -lt 100 ]; do sleep 0.1; i=$((i + 1)); done
  {
    echo "daemon_rss_idle_kb $(rss_kb "$daemon")"
    echo "worker_rss_idle_kb $(worker_rss)"
    ctl put-graph g "$here/g-workers.json" >/dev/null 2>&1
    ctl start g >/dev/null 2>&1
    ctl feed 1 a i64:5 >/dev/null 2>&1
    ctl feed 1 b i64:1 >/dev/null 2>&1
    ctl step 1 >/dev/null 2>&1
    echo "daemon_rss_after_first_step_kb $(rss_kb "$daemon")"
    echo "worker_rss_after_first_step_kb $(worker_rss)"
    i=0
    while [ "$i" -lt 200 ]; do
      ctl feed 1 a "i64:$i" >/dev/null 2>&1
      ctl feed 1 b i64:1 >/dev/null 2>&1
      ctl step 1 >/dev/null 2>&1
      ctl take 1 out >/dev/null 2>&1
      i=$((i + 1))
    done
    echo "daemon_rss_after_200_round_trips_kb $(rss_kb "$daemon")"
    echo "worker_rss_after_200_round_trips_kb $(worker_rss)"
  } > "$1" 2>&1
}

# The figures of the daemon started by start_daemon, taken at the same points for both daemons.
# $1 is the file to write; $2 is the commands that give the daemon its graph, run in order.
daemon_figures() {
  {
    echo "daemon_start_to_first_answer_ns $((answered - started)) resolution_ns $clock_resolution_ns"
    echo "daemon_rss_idle_kb $(rss_kb "$daemon")"
    eval "$2"
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
    # Connections held open: each is a session thread and its writer. The watchers are stopped by
    # the pids taken when they were started.
    vm_kb() { sed -n "s/^$2:[[:space:]]*\([0-9]*\).*/\1/p" "/proc/$1/status"; }
    echo "daemon_vmsize_0_connections_kb $(vm_kb "$daemon" VmSize)"
    watchers=""
    n=0
    while [ "$n" -lt 32 ]; do
      ctl watch all > /dev/null 2>&1 &
      watchers="$watchers $!"
      n=$((n + 1))
    done
    sleep 1
    echo "daemon_rss_32_connections_kb $(rss_kb "$daemon")"
    echo "daemon_vmsize_32_connections_kb $(vm_kb "$daemon" VmSize)"
    echo "daemon_threads_32_connections $(sed -n 's/^Threads:[[:space:]]*//p' "/proc/$daemon/status")"
    for watcher in $watchers; do kill "$watcher" 2>/dev/null; done
    for watcher in $watchers; do wait "$watcher" 2>/dev/null; done
  } > "$1" 2>&1
}

mkdir -p "$here/daemon/plugins"
cp "$here/lib/libdaedalus_dylib_fixture.so" "$here/daemon/plugins/fixture.so"
printf 'listen = ["unix:%s"]\nplugin_dir = "%s"\n' "$here/daemon/d.sock" "$here/daemon/plugins" \
  > "$here/daemon/daedalusd.toml"
start_daemon "$here/daemon/daedalusd.toml" "$out/daedalusd.log" "$here/daemon/d.sock" daedalusd
daemon_figures "$out/daemon.txt" 'ctl put-graph g "$here/g.json" >/dev/null 2>&1; ctl plan g >/dev/null 2>&1'
# The one process this stops is the daemon it started.
stop_daemon

# The lean daemon: the same figures, from the plan the bundle carries. It has no planner, so it is
# given the plan the full daemon made and stores it, then runs it the same way.
if [ -x ./bin/daedalusd-lean ] && [ -s "$here/g.plan" ]; then
  mkdir -p "$here/daemon-lean/plugins"
  cp "$here/lib/libdaedalus_dylib_fixture.so" "$here/daemon-lean/plugins/fixture.so"
  printf 'listen = ["unix:%s"]\nplugin_dir = "%s"\n' "$here/daemon-lean/d.sock" "$here/daemon-lean/plugins" \
    > "$here/daemon-lean/daedalusd.toml"
  start_daemon "$here/daemon-lean/daedalusd.toml" "$out/daedalusd-lean.log" "$here/daemon-lean/d.sock" daedalusd-lean
  daemon_figures "$out/daemon-lean.txt" 'ctl put-plan g "$here/g.plan" >/dev/null 2>&1'
  stop_daemon
else
  echo "the bundle carries no lean daemon or no plan" > "$out/daemon-lean.txt"
fi

# The workers daemon: one Rust fixture worker started from its config, and a graph whose node the
# worker serves. Its own resident memory, and the worker's, are taken at the same points. The
# worker is the one child the daemon has.
if [ -x ./bin/daedalusd-workers ] && [ -x ./bin/worker-fixture ] && [ -s "$here/g-workers.json" ]; then
  mkdir -p "$here/daemon-workers"
  printf 'listen = ["unix:%s"]\nlog_level = "warn"\n\n[[worker]]\nprogram = "%s"\n' \
    "$here/daemon-workers/d.sock" "$here/bin/worker-fixture" > "$here/daemon-workers/daedalusd.toml"
  start_daemon "$here/daemon-workers/daedalusd.toml" "$out/daedalusd-workers.log" "$here/daemon-workers/d.sock" daedalusd-workers
  workers_figures "$out/daemon-workers.txt"
  stop_daemon
else
  echo "the bundle carries no workers daemon, no worker or no graph" > "$out/daemon-workers.txt"
fi
echo done > "$out/finished.txt"
