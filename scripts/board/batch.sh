#!/usr/bin/env bash
# The batched CM5 measurement session: every board step of C1, D2, I2, I3, the ListView fan and
# the GPU records in one go, with the GPU timeout (E11) last.
#
#   scripts/board/batch.sh build      builds the bundle under $CARGO_TARGET_DIR/board/batch
#   scripts/board/batch.sh go         puts it on the board, starts it there detached, waits for
#                                     it, brings the results back, and takes everything off
#
# Settings: BOARD_SSH (where the board is) and BOARD_SSH_CONFIG (an ssh config of its own, as in
# run.sh); REF_TARGET_DIR, the target directory of the reference builds the comparisons run (built from
# the worktrees named in testing/board-runs/C1.md and I3.md, the I3 one through /tmp/dd-i3dev);
# VVL_DIR, an unpacked Khronos validation layer for aarch64; BOARD_LOCKS, the board's GPU lock
# files, space-separated (/run/lock/board-gpu.lock).
#
# A board is somebody's: `go` is run only after its keeper gave the word. The run holds the
# board's GPU locks, writes only under its own /tmp directory, kills only the pid it wrote, and is
# watched at most once a minute. If the board stops answering (a GPU or kernel hang), the run is
# not retried: the script waits, once a minute, for the board to come back by itself.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

target=aarch64-unknown-linux-gnu
out="${CARGO_TARGET_DIR:?set CARGO_TARGET_DIR on NVMe}/board/batch"
dir=/tmp/dd-batch
ssh_to=(ssh -o BatchMode=yes)
[[ -z "${BOARD_SSH_CONFIG:-}" ]] || ssh_to=(ssh -F "$BOARD_SSH_CONFIG" -o BatchMode=yes)
die() { echo "batch: $*" >&2; exit 1; }
on_board() { "${ssh_to[@]}" "${BOARD_SSH:?set BOARD_SSH}" "$@"; }

export ZIG_TARGET=aarch64-linux-gnu.2.36
export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER="$PWD/scripts/board/zigcc.sh"
export CC_aarch64_unknown_linux_gnu="$PWD/scripts/board/zigcc.sh"

tests() { # package, dest prefix: copies each test binary of a package as <prefix>.<target name>
  cargo test --release --no-run --target "$target" -p "$1" --message-format=json 2>>"$out/build.log" |
    python3 -c '
import json, shutil, sys
for line in sys.stdin:
    try:
        m = json.loads(line)
    except ValueError:
        continue
    if m.get("executable") and m.get("profile", {}).get("test"):
        name = "lib" if "lib" in m["target"]["kind"] else m["target"]["name"]
        shutil.copy(m["executable"], sys.argv[1] + "." + name)
' "$out/bundle/bin/$2"
}

build() {
  local release="$CARGO_TARGET_DIR/$target/release" ref="${REF_TARGET_DIR:?set REF_TARGET_DIR}/$target/release"
  rm -rf "$out/bundle"; mkdir -p "$out/bundle/bin" "$out/bundle/vvl"
  : > "$out/build.log"
  cargo build --release --target "$target" -p daedalus-cpu-timings -p daedalus-budget-harness \
    -p daedalus-process-timings -p daedalus-gpu-budgets 2>>"$out/build.log"
  cp "$release"/daedalus-{cpu-timings,budget-harness,process-timings,sweep-worker,gpu-budgets} "$out/bundle/bin/"
  cargo bench --no-run --target "$target" -p daedalus-budget-harness --bench ticks --message-format=json 2>>"$out/build.log" |
    python3 -c 'import json,sys,shutil
for l in sys.stdin:
    try: m=json.loads(l)
    except ValueError: continue
    if m.get("executable") and m["target"]["name"]=="ticks": shutil.copy(m["executable"], sys.argv[1])' "$out/bundle/bin/ticks-bench"
  tests daedalus-wgpu wgpu
  tests daedalus-dma-sys dma-sys
  tests daedalus-vk-sys vk-sys
  tests daedalus-wgpu-conformance wgpu-conformance
  tests daedalus-gpu-budgets gpu-budgets
  (cd testing/fixtures/dylib && cargo build --release --target "$target" 2>>"$out/build.log")
  cp "$CARGO_TARGET_DIR/$target/release/libdaedalus_dylib_fixture.so" "$out/bundle/bin/"
  # The counters and the step runner: static musl builds. zig links it and brings its own C runtime, as run.sh does.
  CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER="$PWD/scripts/board/zigcc.sh" ZIG_TARGET=aarch64-linux-musl \
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_RUSTFLAGS=-Clink-self-contained=no \
    cargo build --release --target aarch64-unknown-linux-musl -p daedalus-counters 2>>"$out/build.log"
  cp "$CARGO_TARGET_DIR/aarch64-unknown-linux-musl/release/counters" "$CARGO_TARGET_DIR/aarch64-unknown-linux-musl/release/bounded" "$out/bundle/bin/"
  cp "$ref/examples/frame_chain" "$out/bundle/bin/ref-frame_chain"
  cp "$(ls -t "$ref"/deps/dylib_frames_overhead-* | grep -v '\.d$' | head -1)" "$out/bundle/bin/ref-dylib_frames_overhead"
  cp "$ref/libdaedalus_plugins_stable_abi.so" "$out/bundle/bin/"
  cp "${VVL_DIR:?set VVL_DIR}"/libVkLayer_khronos_validation.so "$VVL_DIR"/VkLayer_khronos_validation.json "$out/bundle/vvl/"
  cp scripts/board/batch-on-board.sh "$out/bundle/"
  printf '%s\n' '#!/bin/sh' '# Stands in for cargo in the reference handoff test: reports the prebuilt plugin as the build.' \
    'printf '"'"'{"reason":"compiler-artifact","target":{"name":"daedalus_plugins_stable_abi"},"filenames":["%s"]}\n'"'"' "$I3_PLUGIN"' \
    > "$out/bundle/cargo-shim"
  chmod +x "$out/bundle/cargo-shim"
  git rev-parse --short HEAD > "$out/bundle/rev.txt"
  ls "$out/bundle/bin" | wc -l | sed 's/^/batch: binaries in the bundle: /'
}

gpu_check() { # after a reset or a reboot: the GPU answers, from a directory of its own
  local health=/tmp/dd-health
  on_board "test ! -e $health" || die "$health is already on the board: look before removing it"
  tar -C "$out/bundle/bin" -cf - daedalus-gpu-budgets bounded | on_board "mkdir -p $health && tar -C $health -xf -"
  on_board "vulkaninfo --summary 2>&1 | head -20; $health/bounded 60 $health/daedalus-gpu-budgets dispatch-floor" \
    > "$out/results/gpu-after-reset.txt" 2>&1 && echo "batch: the GPU answers after the reset" ||
    echo "batch: the GPU does NOT answer after the reset: tell its keeper"
  on_board "rm -rf $health; test ! -e $health" || die "couldn't remove $health from the board"
}

go() {
  [[ -d "$out/bundle/bin" ]] || die "build the bundle first"
  mkdir -p "$out/results"
  on_board "test ! -e $dir" || die "$dir is already on the board: look before removing it"
  tar -C "$out/bundle" -cf - . | on_board "mkdir -p $dir && tar -C $dir -xf -" ||
    die "couldn't reach the board. If it asked for a password or its host key changed, stop and tell its keeper: type nothing."
  # Detached, so a dropped connection doesn't end it; both GPU locks for the whole run.
  local lock locked=""
  for lock in ${BOARD_LOCKS:-/run/lock/board-gpu.lock}; do locked="$locked flock $lock"; done
  on_board "cd $dir && nohup$locked sh ./batch-on-board.sh > $dir/nohup.txt 2>&1 < /dev/null & echo started"
  local boot answered=yes restarted=no
  boot=$(on_board "cat /proc/sys/kernel/random/boot_id")
  while :; do
    sleep 60
    if on_board "test -e $dir/results/done" 2>/dev/null; then break; fi
    if ! on_board true 2>/dev/null; then answered=no; echo "batch: the board doesn't answer; waiting for it to come back by itself"; continue; fi
    if [[ "$(on_board "cat /proc/sys/kernel/random/boot_id")" != "$boot" ]]; then
      echo "batch: the board restarted during the run (a watchdog reset)"; restarted=yes; break
    fi
    on_board "test -e $dir/run.pid && kill -0 \$(cat $dir/run.pid)" 2>/dev/null || { echo "batch: the run ended without finishing"; break; }
  done
  [[ $answered = yes ]] || echo "batch: the board went away during the run and came back"
  on_board "test -d $dir/results && tar -C $dir/results -cf - ." | tar -C "$out/results" -xf - ||
    echo "batch: no results on the board (a restart clears /tmp)"
  on_board "if [ -f $dir/run.pid ]; then kill \$(cat $dir/run.pid) 2>/dev/null; fi; rm -rf $dir /tmp/dd-i3dev; test ! -e $dir" ||
    die "couldn't remove $dir from the board: remove it by hand"
  # A GPU left wedged (the kernel lives, so no watchdog fires) is never left behind: the board is
  # rebooted and the GPU checked again. A reboot also clears what a non-tmpfs /tmp kept.
  # The board's own last line decides it: "gpu wedged" is a GPU call past its bound, or a failed
  # one with a V3D reset in the kernel log. A check that could not run reboots nothing.
  local verdict
  verdict=$(tail -n 1 "$out/results/gpu-health.txt" 2>/dev/null || true)
  if grep -q "preflight failed" "$out/results/preflight.txt" 2>/dev/null; then
    echo "batch: the preflight failed and nothing was measured:"
    grep -E "^(missing command|does not start|unreadable|bounded does not run)" "$out/results/preflight.txt" || true
  elif [[ "$verdict" != "gpu ok" && "$verdict" != "gpu wedged" ]]; then
    echo "batch: the GPU check gave no answer (${verdict:-no result}); the board is not rebooted for that"
  fi
  if [[ "$verdict" = "gpu wedged" || "${restarted:-no}" = yes ]]; then
    if [[ "$verdict" = "gpu wedged" ]]; then
      echo "batch: the GPU is wedged; rebooting the board"
      on_board "systemctl reboot" || true
      sleep 60
      until on_board true 2>/dev/null; do sleep 60; done
      [[ "$(on_board "cat /proc/sys/kernel/random/boot_id")" != "$boot" ]] || die "the board did not restart: tell its keeper"
    fi
    gpu_check
  fi
  on_board "rm -rf $dir /tmp/dd-i3dev; test ! -e $dir" || die "couldn't remove $dir from the board: remove it by hand"
  on_board "systemctl list-units --type=service --state=active --no-legend --plain" | awk '{print $1}' | sort > "$out/results/services-final.txt" || true
  on_board "systemctl --failed --no-legend --plain" > "$out/results/failed-after.txt" || true
  echo "batch: $dir is gone from the board; results in $out/results"
}

case "${1:-}" in
  build) build ;;
  go) go ;;
  *) sed -n 2,17p "$0"; exit 2 ;;
esac
