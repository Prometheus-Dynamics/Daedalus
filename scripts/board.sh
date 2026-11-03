#!/usr/bin/env bash
# Run a board's Daedalus checks over ssh: cross-build the test binaries here, copy them to the
# board's /tmp, run them there, bring back the logs and the backend-matrix evidence, and remove
# everything from the board. Nothing is built or installed on the board.
#
#   scripts/board.sh PROFILE USER@HOST [RUNNER_NAME]
#
# Profiles (target, build flags, what runs):
#   flame     armv7-unknown-linux-musleabihf, +neon: CPU graph tests, kernel conformance and the
#             CRC-32 throughput (vmull.p8 vs slice-by-8). Flame is the RV1106G2 target; the
#             Luckfox Pico Pro (same chip) stands in for it.
#   k230d     riscv64gc-unknown-linux-musl, +v: the same on real RVV (records the board's VLEN),
#             and the VGLite tests (they skip on an image without libvg_lite.so).
#   rockchip  aarch64-unknown-linux-gnu: the CPU set, plus the GPU crate's tests (Mali through
#             Vulkan), gpu_probe, gpu_bench and the RGA tests (V4L2 mem2mem and librga).
#   raze      aarch64-unknown-linux-gnu: the CPU set and the GPU set, without the RGA.
#
# Output goes to $CARGO_TARGET_DIR/board/RUNNER_NAME/ (logs) and, for the matrix,
# docs/accel-evidence/RUNNER_NAME-DATE.jsonl. GPU runs take /run/lock/raze-gpu.lock when the
# board has it. Cross linkers: zig wrappers in $ZIGCC_DIR (aarch64-cc, armv7-cc, riscv64-cc;
# see docs/testing.md). Written, untested on the flame, k230d and rockchip boards.
set -euo pipefail
cd "$(dirname "$0")/.."

usage() {
  sed -n '2,/^set -euo/p' "$0" | sed '$d; s/^# \{0,1\}//'
  exit 2
}
[[ $# -ge 2 ]] || usage
profile=$1 host=$2
runner=${3:-$profile}
ZIGCC_DIR=${ZIGCC_DIR:-$HOME/zigcc}
TARGET_DIR=${CARGO_TARGET_DIR:-target}
OUT=$TARGET_DIR/board/$runner
rev=$(git rev-parse --short HEAD)
step() { echo "==> $*"; }

gpu=0 rga=0 vglite=0
case $profile in
  flame) target=armv7-unknown-linux-musleabihf cc=armv7-cc flags="-C target-feature=+neon" ;;
  k230d) target=riscv64gc-unknown-linux-musl cc=riscv64-cc flags="-C target-feature=+v" vglite=1 ;;
  rockchip) target=aarch64-unknown-linux-gnu cc=aarch64-cc flags="" gpu=1 rga=1 ;;
  raze) target=aarch64-unknown-linux-gnu cc=aarch64-cc flags="" gpu=1 ;;
  *) usage ;;
esac
[[ -x $ZIGCC_DIR/$cc ]] || { echo "needs the zig wrapper $ZIGCC_DIR/$cc" >&2; exit 1; }
var=$(echo "$target" | tr 'a-z-' 'A-Z_')
export "CARGO_TARGET_${var}_LINKER=$ZIGCC_DIR/$cc" "CC_$(echo "$target" | tr '-' '_')=$ZIGCC_DIR/$cc"
export "CARGO_TARGET_${var}_RUSTFLAGS=$flags"
rustup target list --installed | grep -qx "$target" || rustup target add "$target"

# test_bins CARGO_ARGS... : build test binaries (not run) and print "name path" per binary.
test_bins() {
  cargo test -q --target "$target" "$@" --no-run --message-format=json 2>/dev/null |
    python3 -c 'import json,sys
for line in sys.stdin:
    m = json.loads(line)
    if m.get("reason") == "compiler-artifact" and m.get("executable") and m["profile"]["test"]:
        print(m["target"]["name"], m["executable"])'
}

rm -rf "$OUT" && mkdir -p "$OUT/bin"
step "Building for $target ($profile)"
copy() { cp "$2" "$OUT/bin/$1"; }
while read -r name path; do copy "kernels-$name" "$path"; done \
  < <(test_bins -p daedalus-kernels --test conformance)
while read -r name path; do copy "graph-$name" "$path"; done \
  < <(test_bins -p daedalus-rs --features "engine,plugins,gpu-engine,gpu-mock" \
    --test it --test gpu_fallback --test device_assignment)
if ((gpu)); then
  while read -r name path; do copy "gpu-$name" "$path"; done \
    < <(test_bins -p daedalus-gpu --features "gpu-dmabuf,gpu-vulkan-raw" --lib)
  cargo build -q --release --target "$target" -p daedalus-gpu --features gpu-dmabuf \
    --example gpu_probe --example gpu_bench
  cp "$TARGET_DIR/$target/release/examples/gpu_probe" "$TARGET_DIR/$target/release/examples/gpu_bench" "$OUT/bin/"
fi
if ((rga)); then
  while read -r name path; do copy "rga-$name" "$path"; done \
    < <(test_bins -p daedalus-rga --test v4l2 --test librga)
fi
if ((vglite)); then
  while read -r name path; do copy "vglite-$name" "$path"; done \
    < <(test_bins -p daedalus-vglite --test vglite)
fi
ls "$OUT/bin"

# The script that runs on the board, from its /tmp directory.
cat >"$OUT/bin/run.sh" <<EOF
#!/bin/sh
cd "\$(dirname "\$0")"
export ACCEL_RESULTS_DIR="\$PWD/results" ACCEL_TARGET=$target ACCEL_RUNNER=$runner ACCEL_REV=$rev
status=0
run() { name=\$1; shift; echo "==> \$name"; "\$@" >"logs/\$name.log" 2>&1 || { status=1; echo "FAILED: \$name"; }; tail -n 3 "logs/\$name.log"; }
gpu_lock() { if [ -e /run/lock/raze-gpu.lock ]; then flock /run/lock/raze-gpu.lock "\$@"; else "\$@"; fi; }
mkdir -p logs results
echo "board: \$(uname -a)"; cat /proc/device-tree/model 2>/dev/null; echo
grep -m1 -iE 'isa|features' /proc/cpuinfo || true
# example_imports reads the source tree, which is not on the board.
for t in kernels-* graph-*; do run "\$t" "./\$t" --test-threads 1 --skip example_imports; done
run kernels-throughput ./kernels-conformance --ignored --nocapture --test-threads 1
for t in gpu-*; do [ -e "\$t" ] && run "\$t" gpu_lock "./\$t" --include-ignored --test-threads 1; done
[ -e gpu_probe ] && run gpu_probe gpu_lock ./gpu_probe
[ -e gpu_bench ] && run gpu_bench gpu_lock ./gpu_bench
for t in rga-* vglite-*; do [ -e "\$t" ] && run "\$t" "./\$t" --test-threads 1 --nocapture; done
exit \$status
EOF
chmod +x "$OUT/bin/run.sh"

remote=/tmp/daedalus-board-$rev
step "Running on $host ($remote)"
ssh "$host" "rm -rf $remote && mkdir -p $remote"
scp -q "$OUT"/bin/* "$host:$remote/"
set +e
ssh "$host" "$remote/run.sh" | tee "$OUT/run.log"
status=${PIPESTATUS[0]}
set -e
scp -q -r "$host:$remote/logs" "$host:$remote/results" "$OUT/" || true
ssh "$host" "rm -rf $remote"

evidence=docs/accel-evidence/$runner-$(date +%F).jsonl
if compgen -G "$OUT/results/*.jsonl" >/dev/null; then
  cat "$OUT"/results/*.jsonl >"$evidence"
  step "Evidence: $evidence ($(wc -l <"$evidence") lines); render with scripts/accel.sh"
fi
step "Logs in $OUT/logs; exit $status"
exit "$status"
