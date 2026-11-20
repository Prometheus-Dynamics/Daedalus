#!/usr/bin/env bash
# The batched CM5 measurement session: the figures E13, E9, E14, E6 and E11, in that order, with
# E11 last. The run sheet is testing/board-runs/run-sheet.md; scripts/board/judge.py figures
# applies its pass rules to the results.
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
# run.sh); VVL_DIR, an unpacked Khronos validation layer for aarch64 (libVkLayer_khronos_validation.so
# and VkLayer_khronos_validation.json; not in this repo); BOARD_LOCKS, the board's GPU lock files,
# space-separated (default both, /run/lock/raze-gpu.lock and /run/lock/board-gpu.lock). The
# rehearsal takes, when set, VVL_DIR_NATIVE (an unpacked layer for this machine); without it E13
# says it skipped, and why.
#
# A board is somebody's: `go` is run only after its keeper gave the word. The run holds the
# board's GPU locks, writes only under its own /tmp directory, kills only the pid it wrote, and is
# watched at most once a minute. If the board stops answering (a GPU or kernel hang), the run is
# not retried: the script waits, once a minute, for the board to come back by itself, for at most
# BATCH_RETURN_MINUTES (15). Past that the run ends there, and says so: the board is left as it is
# for its keeper. Nothing here reboots or power-cycles a board; only the user does.
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
tests() { # package, dest prefix, target names...: copies those test binaries as <prefix>.<name>
  local package="$1" prefix="$2"
  shift 2
  cargo test --release --no-run "${target_args[@]}" -p "$package" --message-format=json 2>>"$LOG" |
    python3 -c '
import json, shutil, sys
wanted = set(sys.argv[2:])
for line in sys.stdin:
    try:
        m = json.loads(line)
    except ValueError:
        continue
    if m.get("executable") and m.get("profile", {}).get("test"):
        name = "lib" if "lib" in m["target"]["kind"] else m["target"]["name"]
        if name in wanted:
            shutil.copy(m["executable"], sys.argv[1] + "." + name)
' "$BIN/$prefix" "$@"
}

# The bundle, one list of programs for both: kind board (cross-built for aarch64, `bounded`
# static) or rehearsal (plain native builds of this machine). $2 is the directory the bundle is
# made in. Only what the figures run is in it: the GPU budgets tool, `bounded`, the vk-sys unit
# tests (E13's planted and clean runs) and the wgpu fragment and region tests (E13's clean runs).
bundle() {
  local kind="$1" root="$2" release small vvl
  local BIN="$root/bundle/bin" LOG="$root/build.log" target_args=()
  LEFT_OUT=""
  if [[ "$kind" == board ]]; then
    : "${VVL_DIR:?set VVL_DIR to an unpacked aarch64 Khronos validation layer}"
    target_args=(--target "$target")
    release="$CARGO_TARGET_DIR/$target/release"
    small="$CARGO_TARGET_DIR/aarch64-unknown-linux-musl/release"
    vvl="$VVL_DIR"
    export ZIG_TARGET=aarch64-linux-gnu.2.36
    export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER="$PWD/scripts/board/zigcc.sh"
    export CC_aarch64_unknown_linux_gnu="$PWD/scripts/board/zigcc.sh"
  else
    release="$CARGO_TARGET_DIR/release"
    small="$release"
    vvl="${VVL_DIR_NATIVE:-}"
  fi
  mkdir -p "$root"
  rm -rf "$root/bundle"; mkdir -p "$BIN" "$root/bundle/vvl" "$root/bundle/figures"
  : > "$LOG"
  cargo build --release "${target_args[@]}" -p daedalus-gpu-budgets --bin daedalus-gpu-budgets 2>>"$LOG" ||
    die "the build failed: see $LOG"
  cp "$release/daedalus-gpu-budgets" "$BIN/"
  tests daedalus-wgpu wgpu all region
  tests daedalus-vk-sys vk-sys lib
  # `bounded`: a static musl build on a board (zig brings its C runtime, as run.sh does); a plain
  # build here.
  if [[ "$kind" == board ]]; then
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER="$PWD/scripts/board/zigcc.sh" ZIG_TARGET=aarch64-linux-musl \
      CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_RUSTFLAGS=-Clink-self-contained=no \
      cargo build --release --target aarch64-unknown-linux-musl -p daedalus-counters --bin bounded 2>>"$LOG"
  else
    cargo build --release -p daedalus-counters --bin bounded 2>>"$LOG"
  fi
  cp "$small/bounded" "$BIN/"
  if [[ -n "$vvl" ]]; then
    take "$vvl/libVkLayer_khronos_validation.so" "$root/bundle/vvl/libVkLayer_khronos_validation.so"
    take "$vvl/VkLayer_khronos_validation.json" "$root/bundle/vvl/VkLayer_khronos_validation.json"
  else
    LEFT_OUT="$LEFT_OUT validation-layer"
  fi
  cp scripts/board/batch-on-board.sh "$root/bundle/"
  cp scripts/board/figures/*.sh "$root/bundle/figures/"
  git rev-parse --short HEAD > "$root/bundle/rev.txt"
  ls "$BIN" | wc -l | sed 's/^/batch: programs in the bundle: /'
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
# "gpu ok", every adapter a step opened was lavapipe, and judge.py's figure rules pass in their
# rehearsal form (a step that can't run here may say `skipped:` with its reason).
judge() {
  local r="$1" bad=0 f name badexit steps=0
  if [[ "$(tail -n 1 "$r/preflight.txt" 2>/dev/null)" != "preflight ok" ]]; then
    echo "batch: the preflight did not pass:"; sed 's/^/  /' "$r/preflight.txt" 2>/dev/null
    bad=1
  fi
  [[ -e "$r/done" ]] || { echo "batch: the run did not reach its end (no results/done)"; bad=1; }
  for f in "$r"/*.txt; do
    [[ -e "$f" ]] || continue
    name=$(basename "$f" .txt)
    if grep -qF ": not found" "$f"; then
      echo "batch: $name says ': not found':"; grep -F -m3 ": not found" "$f" | sed 's/^/  /'; bad=1
    fi
    steps=$((steps + $(grep -c '^== step ' "$f" || true)))
    badexit=$(grep '^== exit ' "$f" | grep -vx '== exit 0' || true)
    if [[ -n "$badexit" ]]; then
      echo "batch: $name did not exit 0 ($(echo "$badexit" | head -1)); its first lines:"
      head -n 5 "$f" | sed 's/^/  /'
      bad=1
    fi
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
  echo "batch: rehearsal ran $steps steps"
  [[ -z "${LEFT_OUT:-}" ]] || echo "batch: left out of the rehearsal bundle:$LEFT_OUT"
  python3 scripts/board/judge.py figures --rehearsal "$r" | tee "$r/judge.txt" || bad=1
  if [[ "$bad" == 0 ]]; then echo "batch: verdict: passed"; return 0; fi
  echo "batch: verdict: FAILED"; return 1
}

# The rehearsal: the bundle for this machine, then the whole on-board script run here, from a
# directory of its own under $TMPDIR. Its PATH holds only the board's commands (the list in
# testing/board-runs/cm5-commands.txt), and the GPU is lavapipe, pinned as scripts/gpu-lavapipe.sh
# pins it, with WGPU_BACKEND=vulkan and no EGL vendor so that no backend can reach the real GPU.
# E11 is skipped. Nothing here reaches a board or the grind.
rehearse() {
  local lvp="" run name p verdict=0
  bundle rehearsal "$rehearsal"
  lvp=$(scripts/lavapipe-icd.sh) || lvp=""
  [[ -n "$lvp" ]] || die "rehearse: no lavapipe driver is installed (Mesa's software Vulkan): not run, so the GPU could not be pinned"

  run=$(mktemp -d "${TMPDIR:?set TMPDIR on NVMe}/batch-rehearse.XXXXXX")
  trap 'rm -rf "$run"' EXIT
  mkdir -p "$run/path" "$run/home" "$run/tmp"
  cp -a "$rehearsal/bundle/." "$run/"
  while IFS= read -r name; do
    case "$name" in ''|'#'*) continue ;; esac
    p=$(type -P "$name") || continue
    ln -s "$p" "$run/path/$name"
  done < "$PWD/testing/board-runs/cm5-commands.txt"
  echo "batch: $(ls "$run/path" | wc -l) of the board's commands are on the rehearsal PATH"

  local pinned=(env -i "PATH=$run/path" "HOME=$run/home" "TMPDIR=$run/tmp" LANG=C LC_ALL=C
    "VK_DRIVER_FILES=$lvp" "VK_ICD_FILENAMES=$lvp" "VK_LOADER_LAYERS_DISABLE=~implicit~"
    WGPU_BACKEND=vulkan "__EGL_VENDOR_LIBRARY_FILENAMES=/nonexistent" DAEDALUS_GPU_ADAPTERS=software
    BATCH_REHEARSAL=1 BATCH_SKIP_E11=1 "BATCH_SECONDS=${BATCH_SECONDS:-300}")
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
  for lock in ${BOARD_LOCKS:-/run/lock/raze-gpu.lock /run/lock/board-gpu.lock}; do locked="$locked flock $lock"; done
  # wait.pid is the launcher, which becomes the flock chain: it is alive while the run waits for
  # a lock somebody else holds, before the script writes run.pid.
  on_board "cd $dir && nohup sh -c 'echo \$\$ > $dir/wait.pid; exec$locked sh ./batch-on-board.sh' > $dir/nohup.txt 2>&1 < /dev/null & echo started"
  local boot answered=yes restarted=no away=0
  boot=$(on_board "cat /proc/sys/kernel/random/boot_id")
  while :; do
    sleep 60
    if on_board "test -e $dir/results/done" 2>/dev/null; then break; fi
    if ! on_board true 2>/dev/null; then
      answered=no; away=$((away + 1))
      if ((away > ${BATCH_RETURN_MINUTES:-15})); then
        die "the board has not answered for $away minutes: the run ends here. Tell the coordinator; only the user power-cycles the board."
      fi
      echo "batch: the board doesn't answer ($away min); waiting for it to come back by itself"; continue
    fi
    away=0
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
  on_board "for p in $dir/run.pid $dir/wait.pid; do if [ -f \$p ]; then kill \$(cat \$p) 2>/dev/null; fi; done; rm -rf $dir; test ! -e $dir" ||
    die "couldn't remove $dir from the board: remove it by hand"
  # A GPU left wedged (the kernel lives, so no watchdog fires) ends the session, named: nothing here
  # restarts the board. The board's own last line decides it: "gpu wedged" is a GPU call past its
  # bound, or a failed one with a V3D reset in the kernel log. After a watchdog restart the GPU is
  # checked again.
  local verdict
  verdict=$(tail -n 1 "$out/results/gpu-health.txt" 2>/dev/null || true)
  if grep -q "preflight failed" "$out/results/preflight.txt" 2>/dev/null; then
    echo "batch: the preflight failed and nothing was measured:"
    grep -E "^(missing command|missing program|does not start|unreadable|bounded does not run)" "$out/results/preflight.txt" || true
  elif [[ "$verdict" != "gpu ok" && "$verdict" != "gpu wedged" ]]; then
    echo "batch: the GPU check gave no answer (${verdict:-no result})"
  fi
  if [[ "$verdict" = "gpu wedged" || "${restarted:-no}" = yes ]]; then
    if [[ "$verdict" = "gpu wedged" ]]; then
      die "the GPU is wedged and the kernel lives: the run ends here, and the board is left as it is. Tell the coordinator; only the user restarts or power-cycles the board."
    fi
    gpu_check
  fi
  on_board "rm -rf $dir; test ! -e $dir" || die "couldn't remove $dir from the board: remove it by hand"
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
