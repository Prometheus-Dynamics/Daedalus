#!/bin/sh
# The batched CM5 measurement session, run on the board from its own directory by
# scripts/board/batch.sh. The order is fixed: preflight, E13, E9, E14, E6, ticks (D2 and I2), E11.
# Each figure is a script under figures/ and writes results/<figure>.txt and
# results/<figure>-gate.txt. The run sheet is testing/board-runs/run-sheet.md, and
# scripts/board/judge.py figures applies its rules.
# This script writes nothing outside its directory, starts no service, and kills nothing. The
# caller holds the board's GPU locks for the whole run.
# Two switches are for a rehearsal on another machine (batch.sh rehearse) and are never set on a
# board: BATCH_REHEARSAL=1 lets the validation layer be missing and the GPU be software, and
# BATCH_SKIP_E11=1 skips E11.
# Busybox ash runs this on the board: no bashisms. Checked with `sh -n` and `bash --posix -n`.
D=$(cd "$(dirname "$0")" && pwd)
cd "$D" || exit 1
echo $$ > "$D/run.pid"
R="$D/results"
B="$D/bin"
SECONDS_MAX="${BATCH_SECONDS:-900}"
mkdir -p "$R"
export D R B SECONDS_MAX

# Before anything is measured: every command the scripts name is here, every program in the
# bundle starts, and every script parses. A miss ends the run here, with nothing else touched.
preflight() {
  bad=0
  for s in "$D/batch-on-board.sh" "$D"/figures/*.sh; do
    sh -n "$s" || { echo "the shell cannot parse $(basename "$s")"; bad=1; }
  done
  for c in awk basename cat date dirname dmesg env grep head id ls mkdir paste sed sort \
    systemctl tail uname uniq vulkaninfo; do
    command -v "$c" > /dev/null 2>&1 || { echo "missing command: $c"; bad=1; }
  done
  "$B/bounded" 5 true; code=$?
  [ "$code" = 0 ] || { echo "bounded does not run: exit $code"; echo "preflight failed"; return 1; }
  for n in daedalus-gpu-budgets vk-sys.lib wgpu.all wgpu.region; do
    [ -x "$B/$n" ] || { echo "missing program: $n"; bad=1; }
  done
  for f in "$B"/*; do
    n=$(basename "$f")
    case "$n" in
      bounded) continue ;;
      *.*) set -- "$f" --list ;;
      *) set -- "$f" --help ;;
    esac
    "$B/bounded" 30 "$@" > /dev/null 2>&1 < /dev/null; code=$?
    echo "$n: exit $code"
    # 124 is a hang, 125 to 127 a program that cannot start, above that a signal.
    [ "$code" -lt 124 ] || { echo "does not start: $n"; bad=1; }
  done
  [ "$bad" = 0 ] && echo "preflight ok" || echo "preflight failed"
  return "$bad"
}
if ! preflight > "$R/preflight.txt" 2>&1; then
  date -u +%FT%TZ > "$R/done"
  exit 3
fi

# The board as found: the image and kernel, the dma-heap and its access, the GPU and its driver,
# and the watchdog that decides E11.
{
  uname -a; cat /etc/os-release 2>/dev/null
  echo "user: $(id)"
  ls -l /dev/dma_heap/ /dev/dri/ 2>&1
  cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor 2>/dev/null | sort | uniq -c
  for w in /sys/class/watchdog/watchdog*; do
    [ -d "$w" ] || continue
    for f in identity state timeout timeleft nowayout; do
      [ -r "$w/$f" ] && echo "$w/$f: $(cat "$w/$f")"
    done
  done
  systemctl show -p RuntimeWatchdogUSec -p RebootWatchdogUSec -p WatchdogDevice 2>/dev/null
  for p in /sys/module/v3d/parameters/*; do [ -r "$p" ] && echo "$p: $(cat "$p")"; done
  vulkaninfo --summary 2>/dev/null | head -40
  vulkaninfo 2>/dev/null | grep -E "external_memory_dma_buf|external_fence_fd|external_semaphore_fd" | sort -u
} > "$R/probe.txt" 2>&1
dmesg 2>/dev/null | tail -n 200 > "$R/dmesg-start.txt" || echo "dmesg not readable" > "$R/dmesg-start.txt"
resets_start=$(dmesg 2>/dev/null | grep -c "Resetting GPU")
systemctl list-units --type=service --state=active --no-legend --plain 2>/dev/null |
  awk '{print $1}' | sort > "$R/services-before.txt"

# A board's GPU is hardware, and a software rasteriser must never be measured as one. Only the
# rehearsal runs on lavapipe.
export DAEDALUS_GPU_REQUIRED=1
if [ "${BATCH_REHEARSAL:-}" = 1 ]; then DAEDALUS_GPU_ADAPTERS=software; else DAEDALUS_GPU_ADAPTERS=hardware; fi
export DAEDALUS_GPU_ADAPTERS

# E13 first. Unless the layer named the planted call (or a rehearsal had no layer to try), no
# other GPU figure runs, and each says why.
sh "$D/figures/e13.sh"
e13=$(cat "$R/e13-gate.txt" 2>/dev/null)
case "$e13" in
  open*) go=yes ;;
  skipped*) [ "${BATCH_REHEARSAL:-}" = 1 ] && go=yes || go=no ;;
  *) go=no ;;
esac
for f in e9 e14 e6 ticks e11; do
  # The ticks figure is the CPU's (D2 and I2): it runs whatever E13 said.
  if [ "$go" = yes ] || [ "$f" = ticks ]; then
    sh "$D/figures/$f.sh"
  else
    echo "not run: E13 did not open: $e13" > "$R/$f-gate.txt"
    echo "$f: not run: E13 did not open: $e13" > "$R/$f.txt"
  fi
done

# A V3D wedged while the kernel lives is not the watchdog's. The GPU is checked at the end of
# every run. "gpu wedged" is a GPU call past its bound, or one that failed while the kernel logged
# a V3D reset during the run: the host then ends the session and leaves the board for its keeper.
# A check that could not run is its own answer.
resets_end=$(dmesg 2>/dev/null | grep -c "Resetting GPU")
{ vulkaninfo --summary 2>&1 | head -20
  "$B/bounded" "${BATCH_HEALTH_SECONDS:-60}" "$B/daedalus-gpu-budgets" dispatch-floor 2>&1; code=$?
  echo "== exit $code"
  echo "v3d resets in the kernel log: $resets_start at the start, $resets_end at the end"
  case "$code" in
    0) echo "gpu ok" ;;
    124) echo "gpu wedged" ;;
    125 | 126 | 127) echo "gpu check could not run" ;;
    *) [ "$resets_end" -gt "$resets_start" ] && echo "gpu wedged" || echo "gpu check failed" ;;
  esac
} > "$R/gpu-health.txt"
systemctl list-units --type=service --state=active --no-legend --plain 2>/dev/null |
  awk '{print $1}' | sort > "$R/services-after.txt"
date -u +%FT%TZ > "$R/done"
