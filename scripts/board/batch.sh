#!/usr/bin/env bash
# The batched CM5 measurement session: every board step of C1, D2, I2, I3, the ListView fan and
# the GPU records in one go, with the GPU timeout (E11) last.
#
#   scripts/board/batch.sh build      builds the bundle under $CARGO_TARGET_DIR/board/batch
#   scripts/board/batch.sh rehearse   builds the same programs for this machine and runs the whole
#                                     on-board script here, before `go`: only the board's commands
#                                     on PATH, the GPU on lavapipe, no E11. Its results are under
#                                     $CARGO_TARGET_DIR/board/batch-rehearse/results. It exits
#                                     non-zero unless the rehearsal passes; get a pass first.
#   scripts/board/batch.sh go         puts it on the board, starts it there detached, waits for
#                                     it, brings the results back, and takes everything off
#
# Settings: BOARD_SSH (where the board is) and BOARD_SSH_CONFIG (an ssh config of its own, as in
# run.sh); REF_TARGET_DIR, the target directory of the reference builds the comparisons run (built from
# the worktrees named in testing/board-runs/C1.md and I3.md, the I3 one through /tmp/dd-i3dev);
# VVL_DIR, an unpacked Khronos validation layer for aarch64; BOARD_LOCKS, the board's GPU lock
# files, space-separated (/run/lock/board-gpu.lock). The rehearsal takes, when set, REF_TARGET_DIR_NATIVE
# (the same reference worktrees built natively) and VVL_DIR_NATIVE (an unpacked layer for this
# machine); without them its bundle leaves those out and says so.
#
# A board is somebody's: `go` is run only after its keeper gave the word. The run holds the
# board's GPU locks, writes only under its own /tmp directory, kills only the pid it wrote, and is
# watched at most once a minute. If the board stops answering (a GPU or kernel hang), the run is
# not retried: the script waits, once a minute, for the board to come back by itself.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

target=aarch64-unknown-linux-gnu
out="${CARGO_TARGET_DIR:?set CARGO_TARGET_DIR on NVMe}/board/batch"
rehearsal="$CARGO_TARGET_DIR/board/batch-rehearse"
dir=/tmp/dd-batch
ssh_to=(ssh -o BatchMode=yes)
[[ -z "${BOARD_SSH_CONFIG:-}" ]] || ssh_to=(ssh -F "$BOARD_SSH_CONFIG" -o BatchMode=yes)
die() { echo "batch: $*" >&2; exit 1; }
on_board() { "${ssh_to[@]}" "${BOARD_SSH:?set BOARD_SSH}" "$@"; }
LEFT_OUT=""

# Dynamic scope: tests() writes into the bundle that bundle() is making (BIN, LOG).
tests() { # package, dest prefix: copies each test binary of a package as <prefix>.<target name>
  cargo test --release --no-run "${target_args[@]}" -p "$1" --message-format=json 2>>"$LOG" |
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
' "$BIN/$2"
}

# A file into the bundle. On a board a missing one is an error; for this machine it is left out
# and named, and the steps that need it say so.
take() { # source (may be empty), destination
  if [[ -n "$1" && -e "$1" ]]; then
    cp "$1" "$2"
  elif [[ "$kind" == board ]]; then
    die "missing for the board bundle: ${1:-no source}"
  else
    LEFT_OUT="$LEFT_OUT $(basename "$2")"
  fi
}

# The bundle, one list of programs for both: kind board (cross-built for aarch64, the counters and
# the step runner static) or rehearsal (plain native builds of this machine). $2 is the directory
# the bundle is made in. The list is the same; the sources, the target and what a missing source
# means are the only differences.
bundle() {
  local kind="$1" root="$2" dylib ref release small vvl
  local BIN="$root/bundle/bin" LOG="$root/build.log" target_args=()
  LEFT_OUT=""
  if [[ "$kind" == board ]]; then
    : "${REF_TARGET_DIR:?set REF_TARGET_DIR}" "${VVL_DIR:?set VVL_DIR}"
    target_args=(--target "$target")
    release="$CARGO_TARGET_DIR/$target/release"
    small="$CARGO_TARGET_DIR/aarch64-unknown-linux-musl/release"
    ref="$REF_TARGET_DIR/$target/release"
    vvl="$VVL_DIR"
    export ZIG_TARGET=aarch64-linux-gnu.2.36
    export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER="$PWD/scripts/board/zigcc.sh"
    export CC_aarch64_unknown_linux_gnu="$PWD/scripts/board/zigcc.sh"
  else
    release="$CARGO_TARGET_DIR/release"
    small="$release"
    ref="${REF_TARGET_DIR_NATIVE:+$REF_TARGET_DIR_NATIVE/release}"
    vvl="${VVL_DIR_NATIVE:-}"
  fi
  mkdir -p "$root"
  rm -rf "$root/bundle"; mkdir -p "$BIN" "$root/bundle/vvl"
  : > "$LOG"
  cargo build --release "${target_args[@]}" -p daedalus-cpu-timings -p daedalus-budget-harness \
    -p daedalus-process-timings -p daedalus-gpu-budgets 2>>"$LOG"
  cp "$release"/daedalus-{cpu-timings,budget-harness,process-timings,sweep-worker,gpu-budgets} "$BIN/"
  cargo bench --no-run "${target_args[@]}" -p daedalus-budget-harness --bench ticks --message-format=json 2>>"$LOG" |
    python3 -c 'import json,sys,shutil
for l in sys.stdin:
    try: m=json.loads(l)
    except ValueError: continue
    if m.get("executable") and m["target"]["name"]=="ticks": shutil.copy(m["executable"], sys.argv[1])' "$BIN/ticks-bench"
  tests daedalus-wgpu wgpu
  tests daedalus-dma-sys dma-sys
  tests daedalus-vk-sys vk-sys
  tests daedalus-wgpu-conformance wgpu-conformance
  tests daedalus-gpu-budgets gpu-budgets
  (cd testing/fixtures/dylib && cargo build --release "${target_args[@]}" 2>>"$LOG")
  cp "$release/libdaedalus_dylib_fixture.so" "$BIN/"
  # The counters and the step runner: static musl builds on a board (zig brings its C runtime, as
  # run.sh does); plain builds here.
  if [[ "$kind" == board ]]; then
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER="$PWD/scripts/board/zigcc.sh" ZIG_TARGET=aarch64-linux-musl \
      CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_RUSTFLAGS=-Clink-self-contained=no \
      cargo build --release --target aarch64-unknown-linux-musl -p daedalus-counters 2>>"$LOG"
  else
    cargo build --release -p daedalus-counters 2>>"$LOG"
  fi
  cp "$small/counters" "$small/bounded" "$BIN/"
  # The reference programs. Their native builds exist only if someone made them.
  dylib=""
  if [[ -n "$ref" ]]; then
    dylib=$(ls -t "$ref"/deps/dylib_frames_overhead-* 2>/dev/null | grep -v '\.d$' | head -1) || true
  fi
  take "${ref:+$ref/examples/frame_chain}" "$BIN/ref-frame_chain"
  take "$dylib" "$BIN/ref-dylib_frames_overhead"
  take "${ref:+$ref/libdaedalus_plugins_stable_abi.so}" "$BIN/libdaedalus_plugins_stable_abi.so"
  if [[ -n "$vvl" ]]; then
    take "$vvl/libVkLayer_khronos_validation.so" "$root/bundle/vvl/libVkLayer_khronos_validation.so"
    take "$vvl/VkLayer_khronos_validation.json" "$root/bundle/vvl/VkLayer_khronos_validation.json"
  else
    LEFT_OUT="$LEFT_OUT validation-layer"
  fi
  cp scripts/board/batch-on-board.sh "$root/bundle/"
  printf '%s\n' '#!/bin/sh' '# Stands in for cargo in the reference handoff test: reports the prebuilt plugin as the build.' \
    'printf '"'"'{"reason":"compiler-artifact","target":{"name":"daedalus_plugins_stable_abi"},"filenames":["%s"]}\n'"'"' "$I3_PLUGIN"' \
    > "$root/bundle/cargo-shim"
  chmod +x "$root/bundle/cargo-shim"
  git rev-parse --short HEAD > "$root/bundle/rev.txt"
  ls "$BIN" | wc -l | sed 's/^/batch: binaries in the bundle: /'
  if [[ -n "$LEFT_OUT" ]]; then
    echo "batch: left out of the $kind bundle:$LEFT_OUT"
  fi
}

build() { bundle board "$out"; }

gpu_check() { # after a reset or a reboot: the GPU answers, from a directory of its own
  local health=/tmp/dd-health
  on_board "test ! -e $health" || die "$health is already on the board: look before removing it"
  tar -C "$out/bundle/bin" -cf - daedalus-gpu-budgets bounded | on_board "mkdir -p $health && tar -C $health -xf -"
  on_board "vulkaninfo --summary 2>&1 | head -20; $health/bounded 60 $health/daedalus-gpu-budgets dispatch-floor" \
    > "$out/results/gpu-after-reset.txt" 2>&1 && echo "batch: the GPU answers after the reset" ||
    echo "batch: the GPU does NOT answer after the reset: tell its keeper"
  on_board "rm -rf $health; test ! -e $health" || die "couldn't remove $health from the board"
}

# The judge of a rehearsal: it passes when the preflight passed, the run reached its end, no result
# says "not found" or has a step that exited other than 0 (a timeout is 124), the GPU check said
# "gpu ok", and every adapter a step opened was lavapipe. It names what was skipped and why.
judge() {
  local r="$1" bad=0 f name exits badexit reason steps=0 runs=0 skipped=""
  if [[ "$(tail -n 1 "$r/preflight.txt" 2>/dev/null)" != "preflight ok" ]]; then
    echo "batch: the preflight did not pass:"; sed 's/^/  /' "$r/preflight.txt" 2>/dev/null
    bad=1
  fi
  [[ -e "$r/done" ]] || { echo "batch: the run did not reach its end (no results/done)"; bad=1; }
  for f in "$r"/*.txt; do
    [[ -e "$f" ]] || continue
    name=$(basename "$f" .txt)
    # The shell's and busybox's "cmd: not found". Criterion's "Gnuplot not found" is only a note.
    if grep -qF ": not found" "$f"; then
      echo "batch: $name says ': not found':"; grep -F -m3 ": not found" "$f" | sed 's/^/  /'; bad=1
    fi
    exits=$(grep -c '^== exit ' "$f" || true)
    if [[ "$exits" -gt 0 ]]; then
      steps=$((steps + 1))
      runs=$((runs + $(grep -c '^== 20' "$f" || true)))
      badexit=$(grep '^== exit ' "$f" | grep -vx '== exit 0' || true)
      if [[ -n "$badexit" ]]; then
        echo "batch: $name did not exit 0 ($(echo "$badexit" | head -1)); its first lines:"
        head -n 5 "$f" | sed 's/^/  /'
        bad=1
      fi
    fi
    reason=$(grep -E -m1 'not in the rehearsal bundle|no layer in the rehearsal|skipped: rehearsal' "$f" || true)
    [[ -z "$reason" ]] || skipped="$skipped
  $name: $reason"
  done
  if [[ "$(tail -n 1 "$r/gpu-health.txt" 2>/dev/null)" != "gpu ok" ]]; then
    echo "batch: the GPU check did not say gpu ok: $(tail -n 1 "$r/gpu-health.txt" 2>/dev/null || echo 'no result')"
    bad=1
  fi
  # Every adapter a step opened is lavapipe, and at least one was opened.
  if ! grep -h '^adapter:' "$r"/*.txt 2>/dev/null | grep -qiE 'llvmpipe|lavapipe'; then
    echo "batch: no step reports an adapter that is lavapipe"; bad=1
  fi
  if grep -h '^adapter:' "$r"/*.txt 2>/dev/null | grep -qviE 'llvmpipe|lavapipe'; then
    echo "batch: a step opened an adapter that is not lavapipe:"
    grep -h '^adapter:' "$r"/*.txt | grep -viE 'llvmpipe|lavapipe' | sort -u | sed 's/^/  /'; bad=1
  fi
  echo "batch: rehearsal ran $steps result files with steps ($runs step runs)"
  echo "batch: skipped:${skipped:- none}"
  [[ -z "${LEFT_OUT:-}" ]] || echo "batch: left out of the rehearsal bundle:$LEFT_OUT"
  if [[ "$bad" == 0 ]]; then echo "batch: verdict: passed"; return 0; fi
  echo "batch: verdict: FAILED"; return 1
}

# The rehearsal: the bundle for this machine, then the whole on-board script run here, from a
# directory of its own under $TMPDIR. Its PATH holds only the board's commands (the list in
# testing/board-runs/cm5-commands.txt), and the GPU is lavapipe, pinned as scripts/gpu-lavapipe.sh
# pins it, with WGPU_BACKEND=vulkan and no EGL vendor so that no backend can reach the real GPU.
# E11 is skipped. Nothing here reaches a board or the grind.
rehearse() {
  local lvp="" candidate run name p verdict=0
  bundle rehearsal "$rehearsal"
  for candidate in /usr/share/vulkan/icd.d/lvp_icd.*.json /usr/share/vulkan/icd.d/lvp_icd.json \
    /etc/vulkan/icd.d/lvp_icd.*.json; do
    if [[ -f "$candidate" && "$candidate" != *i686* ]]; then lvp="$candidate"; break; fi
  done
  [[ -n "$lvp" ]] || die "rehearse: no lavapipe driver is installed (Mesa's software Vulkan): not run, so the GPU could not be pinned"

  run=$(mktemp -d "${TMPDIR:?set TMPDIR on NVMe}/batch-rehearse.XXXXXX")
  trap 'rm -rf "$run"' EXIT
  mkdir -p "$run/path" "$run/home" "$run/tmp" "$run/i3"
  cp -a "$rehearsal/bundle/." "$run/"
  while IFS= read -r name; do
    case "$name" in ''|'#'*) continue ;; esac
    p=$(type -P "$name") || continue
    ln -s "$p" "$run/path/$name"
  done < "$PWD/testing/board-runs/cm5-commands.txt"
  echo "batch: $(ls "$run/path" | wc -l) of the board's commands are on the rehearsal PATH"

  local pinned=(env -i "PATH=$run/path" "HOME=$run/home" "TMPDIR=$run/tmp" LANG=C LC_ALL=C
    "VK_DRIVER_FILES=$lvp" "VK_ICD_FILENAMES=$lvp" "VK_LOADER_LAYERS_DISABLE=~implicit~"
    WGPU_BACKEND=vulkan "__EGL_VENDOR_LIBRARY_FILENAMES=/nonexistent"
    BATCH_REHEARSAL=1 BATCH_SKIP_E11=1 "BATCH_I3_DIR=$run/i3" "BATCH_SECONDS=${BATCH_SECONDS:-300}")
  local summary
  summary=$("${pinned[@]}" vulkaninfo --summary 2>&1 || true)
  grep -qiE 'llvmpipe|lavapipe' <<< "$summary" ||
    die "rehearse: vulkaninfo under the rehearsal's environment does not show lavapipe; nothing was run"

  "${pinned[@]}" "$run/path/sh" "$run/batch-on-board.sh" > "$run/nohup.txt" 2>&1 < /dev/null || true
  rm -rf "$rehearsal/results"; mkdir -p "$rehearsal/results"
  cp -a "$run/results/." "$rehearsal/results/" 2>/dev/null || true
  cp "$run/nohup.txt" "$rehearsal/nohup.txt"
  judge "$rehearsal/results" || verdict=$?
  rm -rf "$run"; trap - EXIT
  [[ "$verdict" == 0 ]] || exit 1
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
  # wait.pid is the launcher, which becomes the flock chain: it is alive while the run waits for
  # a lock somebody else holds, before the script writes run.pid.
  on_board "cd $dir && nohup sh -c 'echo \$\$ > $dir/wait.pid; exec$locked sh ./batch-on-board.sh' > $dir/nohup.txt 2>&1 < /dev/null & echo started"
  local boot answered=yes restarted=no
  boot=$(on_board "cat /proc/sys/kernel/random/boot_id")
  while :; do
    sleep 60
    if on_board "test -e $dir/results/done" 2>/dev/null; then break; fi
    if ! on_board true 2>/dev/null; then answered=no; echo "batch: the board doesn't answer; waiting for it to come back by itself"; continue; fi
    if [[ "$(on_board "cat /proc/sys/kernel/random/boot_id")" != "$boot" ]]; then
      echo "batch: the board restarted during the run (a watchdog reset)"; restarted=yes; break
    fi
    # Alive is the script itself, or, before it has a lock, the launcher waiting for one.
    on_board "if [ -e $dir/run.pid ]; then kill -0 \$(cat $dir/run.pid); else kill -0 \$(cat $dir/wait.pid); fi" 2>/dev/null ||
      { echo "batch: the run ended without finishing"; break; }
  done
  [[ $answered = yes ]] || echo "batch: the board went away during the run and came back"
  on_board "test -d $dir/results && tar -C $dir/results -cf - ." | tar -C "$out/results" -xf - ||
    echo "batch: no results on the board (a restart clears /tmp)"
  on_board "for p in $dir/run.pid $dir/wait.pid; do if [ -f \$p ]; then kill \$(cat \$p) 2>/dev/null; fi; done; rm -rf $dir /tmp/dd-i3dev; test ! -e $dir" ||
    die "couldn't remove $dir from the board: remove it by hand"
  # A GPU left wedged (the kernel lives, so no watchdog fires) is never left behind: the board is
  # rebooted and the GPU checked again. A reboot also clears what a non-tmpfs /tmp kept.
  # The board's own last line decides it: "gpu wedged" is a GPU call past its bound, or a failed
  # one with a V3D reset in the kernel log. A check that could not run reboots nothing.
  local verdict
  verdict=$(tail -n 1 "$out/results/gpu-health.txt" 2>/dev/null || true)
  if grep -q "preflight failed" "$out/results/preflight.txt" 2>/dev/null; then
    echo "batch: the preflight failed and nothing was measured:"
    grep -E "^(missing command|missing program|does not start|unreadable|bounded does not run)" "$out/results/preflight.txt" || true
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
  rehearse) rehearse ;;
  go) go ;;
  *) sed -n '2,/^set /p' "$0" | sed '$d'; exit 2 ;;
esac
