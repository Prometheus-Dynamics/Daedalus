#!/usr/bin/env bash
# One command for a board run: build, put it on the board, run it, bring the results back, take
# everything off the board again, and judge.
#
#   scripts/board/run.sh local            the same bundle on this machine: proves the run and
#                                         the judge without a board
#   scripts/board/run.sh luckfox-build    only builds what a Luckfox run would send
#   scripts/board/run.sh luckfox --go     a Luckfox (armv7 Linux, musl), over ssh
#   scripts/board/run.sh pico --go        a Raspberry Pi Pico (RP2040), through a debug probe
#
# A board is somebody's. Nobody runs on one on their own initiative: `--go` says the board's
# keeper gave the word for this run, and without it nothing touches a board.
#
# Settings, from the environment:
#   BOARD_SSH       where the Linux board is, as `ssh` takes it. It comes from your own ssh
#                   config: this script adds no option that weakens a host-key or login check.
#   BOARD_SECONDS   the longest one test binary may run on the board (300), or how long the
#                   Pico's log is read (60).
#   BOARD_PACKAGES  the packages whose tests go in the bundle.
#
# What it leaves: results under $CARGO_TARGET_DIR/board/<board>/, and nothing on the board. It
# kills only the processes it started, by pid.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
cd ..

board="${1:-}"
go="${2:-}"
seconds="${BOARD_SECONDS:-}"
packages="${BOARD_PACKAGES:-daedalus-runner daedalus-cpu daedalus-host}"
target_dir="${CARGO_TARGET_DIR:-target}"
out="$target_dir/board/$board"

die() {
  echo "board: $*" >&2
  exit 2
}

need_go() {
  [[ "$go" == "--go" ]] || die "this would run on a real board. Ask its keeper first, and pass --go when they have said yes."
}

bundle() { # rust target: builds the test binaries and gathers them under $out/bundle/bin
  local target="$1" upper flags=()
  rm -rf "$out"
  mkdir -p "$out/bundle/bin" "$out/results"
  for package in $packages; do flags+=(-p "$package"); done
  upper=$(tr '[:lower:]-' '[:upper:]_' <<<"$target")
  local env=()
  if [[ "$target" == *-musl* && "$target" != "$(rustc -vV | sed -n 's/^host: //p')" ]]; then
    # zig links for the board, and brings its own C runtime.
    local zig
    case "$target" in
      armv7-unknown-linux-musleabihf) zig=arm-linux-musleabihf ;;
      aarch64-unknown-linux-musl) zig=aarch64-linux-musl ;;
      riscv64gc-unknown-linux-musl) zig=riscv64-linux-musl ;;
      *) die "no zig target is known for $target" ;;
    esac
    env=(
      "ZIG_TARGET=$zig"
      "CARGO_TARGET_${upper}_LINKER=$PWD/scripts/zigcc.sh"
      "CARGO_TARGET_${upper}_RUSTFLAGS=-Clink-self-contained=no"
    )
  fi
  echo "board: building the tests of $packages for $target"
  env "${env[@]}" cargo test --release --no-run --target "$target" "${flags[@]}" \
    --message-format=json 2>"$out/build.log" |
    python3 -c '
import json, os, shutil, sys
dest = sys.argv[1]
for line in sys.stdin:
    try:
        message = json.loads(line)
    except ValueError:
        continue
    exe = message.get("executable")
    if exe and message.get("profile", {}).get("test"):
        # Two packages may each have a test of one name, so the package is part of the name.
        package = message["package_id"].rsplit("#", 1)[-1].split("@")[0].rsplit("/", 1)[-1]
        shutil.copy(exe, os.path.join(dest, package + "." + message["target"]["name"]))
' "$out/bundle/bin" || { tail -20 "$out/build.log" >&2; die "the build failed: $out/build.log"; }
  [[ -n "$(ls "$out/bundle/bin")" ]] || { tail -20 "$out/build.log" >&2; die "no test binary was built"; }
  cp scripts/board/on-board.sh "$out/bundle/"
  git rev-parse --short HEAD > "$out/results/rev.txt" 2>/dev/null || true
  echo "board: $(ls "$out/bundle/bin" | wc -l) test binaries in the bundle"
}

run_local() {
  bundle "$(rustc -vV | sed -n 's/^host: //p')"
  local work
  work="$(mktemp -d)"
  cp -r "$out/bundle/." "$work/"
  # From a directory of its own, as on a board: a test that needs the source tree shows here.
  (cd "$work" && sh ./on-board.sh "${seconds:-300}")
  cp -r "$work/results/." "$out/results/"
  rm -rf "$work"
}

run_linux() { # rust target
  need_go
  local where="${BOARD_SSH:-}"
  [[ -n "$where" ]] || die "set BOARD_SSH to where the board is, as ssh takes it"
  bundle "$1"
  local ssh=(ssh -o BatchMode=yes "$where")
  local dir="/tmp/daedalus-board-$$"
  echo "board: putting the bundle on $where at $dir"
  tar -C "$out/bundle" -cf - . | "${ssh[@]}" "mkdir -p $dir && tar -C $dir -xf -" ||
    die "couldn't reach the board. If it asked for a password or its host key changed, stop and tell its keeper: type nothing."
  echo "board: running, at most ${seconds:-300} s a binary"
  # One retry of nothing: a run that drops is reported, and the directory is still removed below.
  "${ssh[@]}" "sh $dir/on-board.sh ${seconds:-300}" || echo "board: the run ended early"
  "${ssh[@]}" "tar -C $dir/results -cf - ." | tar -C "$out/results" -xf - ||
    echo "board: the results couldn't be fetched"
  "${ssh[@]}" "rm -rf $dir && test ! -e $dir" || die "couldn't remove $dir from the board: remove it by hand"
  echo "board: $dir is gone from the board"
}

run_pico() {
  need_go
  command -v probe-rs >/dev/null || die "probe-rs isn't installed: cargo install probe-rs-tools"
  rm -rf "$out"
  mkdir -p "$out/results"
  echo "board: building the Pico firmware"
  cargo build --target thumbv6m-none-eabi --profile chip -p daedalus-chip-board 2>"$out/build.log" ||
    { tail -20 "$out/build.log" >&2; die "the build failed: $out/build.log"; }
  local firmware="$target_dir/thumbv6m-none-eabi/chip/daedalus-chip-pico"
  [[ -f "$firmware" ]] || die "no firmware at $firmware"
  echo "board: flashing, and reading the log for ${seconds:-60} s"
  probe-rs run --chip RP2040 "$firmware" >"$out/results/pico.log" 2>&1 &
  local pid=$!
  sleep "${seconds:-60}"
  # The one process this kills is the probe it started.
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

case "$board" in
  local)
    run_local
    python3 scripts/board/judge.py linux "$out/results"
    ;;
  luckfox-build)
    # Only the build, which needs no board: what would be sent is left in the bundle.
    bundle armv7-unknown-linux-musleabihf
    file "$out"/bundle/bin/* | head -3
    ;;
  luckfox)
    run_linux armv7-unknown-linux-musleabihf
    python3 scripts/board/judge.py linux "$out/results"
    ;;
  pico)
    run_pico
    python3 scripts/board/judge.py pico "$out/results/pico.log"
    ;;
  *)
    sed -n '2,25p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit 2
    ;;
esac
