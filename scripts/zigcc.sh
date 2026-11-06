#!/usr/bin/env bash
# scripts/zigcc.sh DIR: write the zig linker wrappers the cross targets use into DIR
# (riscv64-cc, armv7-cc, aarch64-musl-cc; see docs/testing.md). Each drops the flags zig's
# frontend rejects and pins the musl target. Uses `zig` from PATH, else `python3 -m ziglang`
# (pip's ziglang); exits 1 when neither is there.
set -euo pipefail
dir=${1:?usage: scripts/zigcc.sh DIR}
if command -v zig >/dev/null; then
  zig="zig"
elif python3 -m ziglang version >/dev/null 2>&1; then
  zig="python3 -m ziglang"
else
  echo "zigcc.sh: needs zig on PATH or python3 -m ziglang" >&2
  exit 1
fi
mkdir -p "$dir"
for pair in "riscv64-cc riscv64-linux-musl" "armv7-cc arm-linux-musleabihf" \
  "aarch64-musl-cc aarch64-linux-musl"; do
  set -- $pair
  cat > "$dir/$1" <<SH
#!/bin/sh
args=""
for a in "\$@"; do case "\$a" in --target=*|-fuse-ld=lld|-B*gcc-ld|-march=*|-mabi=*|-mfpu=*|-mfloat-abi=*|-Wl,--fix-cortex-a53-843419) ;; *) args="\$args '\$a'";; esac; done
eval exec $zig cc -target $2 \$args
SH
  chmod +x "$dir/$1"
done
