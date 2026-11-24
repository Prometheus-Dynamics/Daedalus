#!/usr/bin/env bash
# Makes the lander's working area from nothing, or fills in what is missing. Safe to rerun.
#
#   scripts/dev/setup.sh [--timer] [--board]
#
# DAEDALUS_WORK (default /mnt/nvme-wd/daedalus) gets:
#   wt/work     a clone of next to work in
#   wt/gate     a clone of next that land.sh gates and lands from
#   wt/seeder   a clone of next that reseed.sh builds the seed from
#   target/     the build directories, one a checkout (scripts/dev/env.sh)
#   cache/      the seed, the validation layer, TMPDIR: all of it can be thrown away
#   logs/       the gate logs
# --timer installs the user timer that keeps the seed current (dd-reseed, scripts/dev/reseed.sh).
# --board also builds the aarch64 validation layer a board session runs under
# (testing/board-runs/run-sheet.md).
# It builds the validation layer the vvl steps run under when the cache has none for the pinned
# inputs (cache/vvl/<scripts/dev/vvl-key.sh>).
set -euo pipefail
work="${DAEDALUS_WORK:-/mnt/nvme-wd/daedalus}"
url="https://github.com/Prometheus-Dynamics/Daedalus.git"
mkdir -p "$work"/{wt,target,cache/tmp,logs}
here="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
name="$(git -C "$here" config user.name || true)"; email="$(git -C "$here" config user.email || true)"
for clone in work gate seeder; do
  dir="$work/wt/$clone"
  [[ -d "$dir/.git" ]] && continue
  git clone -q --branch next "$url" "$dir"
  [[ -n "$name" ]] && git -C "$dir" config user.name "$name"
  [[ -n "$email" ]] && git -C "$dir" config user.email "$email"
  echo "setup: cloned $dir"
done
# The validation layer the vvl steps run under: a cache, built once for its pinned inputs. It
# downloads the layer's sources and dependencies (scripts/vvl.pin names them).
layer="$work/cache/vvl/$("$here/scripts/dev/vvl-key.sh")"
if [[ ! -f "$layer/x86_64/libVkLayer_khronos_validation.so" ]]; then
  echo "setup: building the validation layer into $layer"
  "$here/scripts/vvl-build.sh" --arch x86_64 --out "$layer/x86_64" --work "$layer/work"
fi
if [[ " $* " == *" --board "* && ! -f "$layer/aarch64/libVkLayer_khronos_validation.so" ]]; then
  echo "setup: building the board's validation layer into $layer"
  "$here/scripts/vvl-build.sh" --arch aarch64 --out "$layer/aarch64" --work "$layer/work"
fi
if [[ " $* " == *" --timer "* ]]; then
  units="$HOME/.config/systemd/user"
  mkdir -p "$units"
  sed "s|@WORK@|$work|g" "$here/scripts/dev/dd-reseed.service" > "$units/dd-reseed.service"
  cp "$here/scripts/dev/dd-reseed.timer" "$units/dd-reseed.timer"
  systemctl --user daemon-reload
  systemctl --user enable --now dd-reseed.timer
  echo "setup: dd-reseed.timer on"
fi
