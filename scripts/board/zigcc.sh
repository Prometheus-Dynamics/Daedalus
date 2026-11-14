#!/bin/sh
# A linker for a board's cross build. `ZIG_TARGET` says which: arm-linux-musleabihf,
# aarch64-linux-musl, or with a glibc's version, aarch64-linux-gnu.2.36. rustc passes a few
# flags zig's driver doesn't take, which are dropped.
args=""
for a in "$@"; do
  case "$a" in
    --target=*|-fuse-ld=lld|-B*gcc-ld|-march=*|-Wl,--fix-cortex-a53-843419) ;;
    *) args="$args '$a'" ;;
  esac
done
eval exec zig cc -target "${ZIG_TARGET:?}" $args
