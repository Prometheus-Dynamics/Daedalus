#!/bin/sh
# The batched CM5 measurement session, run on the board from its own directory by
# scripts/board/batch.sh. Each step writes results/<step>.txt; results/done is written last.
# It writes nothing outside its directory, starts no service, and kills only what it started.
# The caller holds the board's GPU locks for the whole run.
D=$(cd "$(dirname "$0")" && pwd)
cd "$D" || exit 1
echo $$ > "$D/run.pid"
R="$D/results"
mkdir -p "$R"
B="$D/bin"
SECONDS_MAX="${BATCH_SECONDS:-900}"

I3_DIR="${BATCH_I3_DIR:-/tmp/dd-i3dev}"

# The image may have neither `timeout` nor `taskset`, so the bundle brings `bounded`, which does
# both. It exits 124 past the bound, 126 or 127 for a command that cannot run.
run_step() { # name, bounded's options..., --, command...
  name="$1"; shift
  pin=""
  while [ "$1" != -- ]; do pin="$pin $1"; shift; done
  shift
  echo "== $name: $*" >> "$R/steps.txt"
  # shellcheck disable=SC2086 # the options are words
  { echo "== $(date -u +%FT%TZ) $*"; "$B/bounded" $pin "$SECONDS_MAX" "$@" 2>&1; echo "== exit $?"; } >> "$R/$name.txt"
}
step() { name="$1"; shift; run_step "$name" -- "$@"; }          # name, command...: bounded, logged
pinned() { name="$1"; shift; run_step "$name" -c 2 -- "$@"; }   # the same, on core 2

# Before anything is measured: every command this script names is on the board, and every
# program in the bundle starts. A miss ends the run here, with nothing else touched.
preflight() {
  bad=0
  # The board's own shell reads the whole script first: it is busybox ash there, and the test at
  # home runs under another shell.
  sh -n "$D/batch-on-board.sh" || { echo "the shell cannot parse the script"; bad=1; }
  for c in awk basename cat date dirname dmesg env findmnt grep head mkdir paste rmdir sed sort \
    systemctl tail uname uniq vulkaninfo; do
    command -v "$c" > /dev/null 2>&1 || { echo "missing command: $c"; bad=1; }
  done
  "$B/bounded" 5 true; code=$?
  [ "$code" = 0 ] || { echo "bounded does not run: exit $code"; echo "preflight failed"; return 1; }
  for f in "$B"/*; do
    n=$(basename "$f")
    case "$n" in
      bounded) continue ;;
      *.so) [ -r "$f" ] || { echo "unreadable: $n"; bad=1; }; continue ;;
      counters) set -- "$f" true ;;
      ref-frame_chain) set -- env FRAME_CHAIN_TICKS=1 "$f" ;;
      daedalus-process-timings | daedalus-sweep-worker) set -- "$f" ;;
      ticks-bench | ref-dylib_frames_overhead | *.*) set -- "$f" --list ;;
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

kernel_log() { # name: what the kernel said since the mark, when the board lets a user read it
  dmesg 2>/dev/null | tail -n 200 > "$R/dmesg-$1.txt" || echo "dmesg not readable" > "$R/dmesg-$1.txt"
}

# 0. The board as found, and the watchdog that decides the V3D timeout step.
{
  uname -a; cat /etc/os-release 2>/dev/null
  cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor 2>/dev/null | sort | uniq -c
  findmnt /tmp 2>/dev/null
  for w in /sys/class/watchdog/watchdog*; do
    [ -d "$w" ] || continue
    for f in identity state timeout timeleft status nowayout; do
      [ -r "$w/$f" ] && echo "$w/$f: $(cat "$w/$f")"
    done
  done
  systemctl show -p RuntimeWatchdogUSec -p RebootWatchdogUSec -p WatchdogDevice 2>/dev/null
  grep -o 'bcm2835_wdt[^ ]*\|watchdog[^ ]*' /proc/cmdline 2>/dev/null
  for p in /sys/module/v3d/parameters/*; do [ -r "$p" ] && echo "$p: $(cat "$p")"; done
  cat /proc/sys/kernel/panic /proc/sys/kernel/hung_task_panic /proc/sys/kernel/softlockup_panic 2>/dev/null |
    paste -sd' ' | sed 's/^/panic hung_task_panic softlockup_panic: /'
  vulkaninfo --summary 2>/dev/null | head -40
  # The device extensions and the feature the import and fence paths need (E13).
  vulkaninfo 2>/dev/null | grep -E "external_memory_dma_buf|external_fence_fd|storageBuffer8BitAccess" | sort -u
} > "$R/probe.txt" 2>&1
systemctl list-units --type=service --state=active --no-legend --plain 2>/dev/null |
  awk '{print $1}' | sort > "$R/services-before.txt"
kernel_log start
resets_start=$(dmesg 2>/dev/null | grep -c "Resetting GPU")

# 1. C1: next's byte chain against the reference frame chain (C1.md), pinned to core 2.
for round in 1 2 3; do
  pinned c1-byte-chain "$B/daedalus-cpu-timings" byte_chain 5000
  step c1-ref-fused env FRAME_CHAIN_TICKS=5000 FRAME_BENCH_CPU=2 "$B/ref-frame_chain"
  step c1-ref-unfused env DAEDALUS_NODE_FUSION=0 FRAME_CHAIN_TICKS=5000 FRAME_BENCH_CPU=2 "$B/ref-frame_chain"
done

# 2. C1/D2: per-stage and idle tick; D2/I2: instructions per tick from the counters, as the
#    difference of two tick counts over the extra ticks.
for round in 1 2 3; do
  pinned d2-chain16 "$B/daedalus-budget-harness" chain 2000000 16 unfused
  pinned d2-chain1 "$B/daedalus-budget-harness" chain 2000000 1 unfused
  pinned d2-chain16-fused "$B/daedalus-budget-harness" chain 2000000 16 fused
  pinned d2-idle "$B/daedalus-budget-harness" idle 2000000
  for ticks in 100000 1100000; do
    pinned d2-counters-idle "$B/counters" "$B/daedalus-budget-harness" idle $ticks
    pinned d2-counters-chain16 "$B/counters" "$B/daedalus-budget-harness" chain $ticks 16 unfused
    pinned d2-counters-chain16-fused "$B/counters" "$B/daedalus-budget-harness" chain $ticks 16 fused
  done
done

# 3. I3: next's rows at 3,110,400 bytes, and the reference handoff test (I3.md), static and stable. That test
#    builds its plugin with $CARGO; the shim answers with the plugin built beside it, and the
#    test runs its build from the workspace path it was compiled in, which is under /tmp.
step i3-allocations "$B/daedalus-budget-harness" allocations
# Next's stable row: the same value through the dylib fixture and back, by reference.
step i3-stable-allocations "$B/daedalus-budget-harness" plugin 0 "$B/libdaedalus_dylib_fixture.so" allocations
for round in 1 2 3; do
  step i3-ticks "$B/ticks-bench" --bench i3_
  step i3-stable "$B/daedalus-budget-harness" plugin 5000 "$B/libdaedalus_dylib_fixture.so" i3
done
mkdir -p "$I3_DIR"
for round in 1 2 3; do
  step i3-ref env I3_PLUGIN="$B/libdaedalus_plugins_stable_abi.so" CARGO="$D/cargo-shim" \
    "$B/ref-dylib_frames_overhead" frame_handoff_overhead --ignored --nocapture
done
rmdir "$I3_DIR" 2>/dev/null

# 4. The records fan (ListView): every fan row, three rounds.
for round in 1 2 3; do
  step listview-fanout "$B/daedalus-process-timings" fanout "$B/daedalus-sweep-worker" 301
done

# 5. GPU on V3D: the tests, then the timings of E2, E3, E5, E7, E8, E9 and E12.
export DAEDALUS_GPU_REQUIRED=1
step gpu-test-storage env DAEDALUS_DMA_HEAP_REQUIRED=1 "$B/wgpu.storage" --nocapture --test-threads=1
step gpu-test-transfer "$B/wgpu.transfer" --nocapture --test-threads=1
step gpu-test-region "$B/wgpu.region" --nocapture --test-threads=1
step gpu-test-fragment "$B/wgpu.fragment" --nocapture --test-threads=1
step gpu-test-fences "$B/wgpu.lib" fence_tests --nocapture --test-threads=1
step gpu-test-dma-sys "$B/dma-sys.lib" --nocapture --test-threads=1
step gpu-test-dma-kernel "$B/dma-sys.kernel" --nocapture --test-threads=1
step gpu-test-vk-sys "$B/vk-sys.lib" --nocapture --test-threads=1
for t in "$B"/wgpu-conformance.* "$B"/gpu-budgets.*; do
  [ -x "$t" ] && step gpu-test-$(basename "$t") "$t" --nocapture --test-threads=1
done
for round in 1 2 3; do
  step gpu-budgets "$B/daedalus-gpu-budgets" limits alloc16 transfers cached region-batch raw-submit \
    fragment-pass fences timer-source busy dma-1mib heap-read batching allocation dispatch-floor readback elements
done

# 6. Vulkan validation with synchronization validation (E7, E13), through the layer in ./vvl.
export VK_LAYER_PATH="$D/vvl" VK_INSTANCE_LAYERS=VK_LAYER_KHRONOS_validation
export VK_KHRONOS_VALIDATION_VALIDATE_SYNC=1 VK_LAYER_VALIDATE_SYNC=1
step validation-vk-sys "$B/vk-sys.lib" --nocapture --test-threads=1
step validation-fragment "$B/wgpu.fragment" --nocapture --test-threads=1
step validation-region "$B/wgpu.region" --nocapture --test-threads=1
step validation-raw-submit "$B/daedalus-gpu-budgets" raw-submit fragment-pass
unset VK_LAYER_PATH VK_INSTANCE_LAYERS VK_KHRONOS_VALIDATION_VALIDATE_SYNC VK_LAYER_VALIDATE_SYNC
kernel_log gpu

# 7. E11 last: a real GPU timeout on V3D, ramped from short work up, and only when a hardware
#    watchdog is armed and fed by systemd, so a wedged kernel returns the board by itself.
armed=no
for w in /sys/class/watchdog/watchdog*; do
  [ "$(cat "$w/state" 2>/dev/null)" = active ] && armed=yes
done
runtime=$(systemctl show -p RuntimeWatchdogUSec --value 2>/dev/null)
case "$runtime" in ""|0|infinity) armed=no ;; esac
echo "watchdog armed: $armed (RuntimeWatchdogUSec=$runtime)" > "$R/e11-gate.txt"
if [ "$armed" = yes ]; then
  for rounds in 1000000 10000000 100000000 1000000000 4000000000; do
    step e11-ramp env DAEDALUS_GRIND_ROUNDS=$rounds DAEDALUS_GRIND_REPEAT=1 "$B/daedalus-gpu-budgets" grind
    grep -q "the wait failed" "$R/e11-ramp.txt" && break
  done
  if ! grep -q "the wait failed" "$R/e11-ramp.txt"; then
    step e11-ramp env DAEDALUS_GRIND_REPEAT=4 "$B/daedalus-gpu-budgets" grind
  fi
  # V3D's driver resets a hung job after about half a second, before a 5 s bound. A bound under
  # that reaches the wait's own timeout path on the device.
  if ! grep -q "the wait failed" "$R/e11-ramp.txt"; then
    step e11-short-bound env DAEDALUS_GRIND_BOUND_MS=100 DAEDALUS_GRIND_ROUNDS=4000000000 \
      DAEDALUS_GRIND_REPEAT=1 "$B/daedalus-gpu-budgets" grind
  fi
  kernel_log e11
else
  echo "E11 on V3D not run: no armed hardware watchdog fed by systemd" >> "$R/e11-gate.txt"
fi

# A V3D wedged while the kernel lives is not the watchdog's. The GPU is checked at the end of
# every run, E11 or not, so a wait that failed in any step is covered. The host reboots the board
# only on "gpu wedged": the GPU call ran past its bound, or it failed and the kernel logged a
# V3D reset during the run. A check that could not run is its own answer and reboots nothing.
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
