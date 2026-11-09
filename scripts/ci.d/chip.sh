#!/usr/bin/env bash
# Builds the blink firmware for each chip target and checks its size.
#
# Compiled size is pinned exactly, so any change has to update this file with a reason. The pins
# are "flash:ram" in bytes, from `.vector_table + .text + .rodata + .data` and
# `.data + .bss + .uninit`, and are in docs/chips.md.
set -euo pipefail

readonly MAX_FLASH=12288
readonly MAX_RAM=584
readonly TARGETS=(
  "thumbv7em-none-eabihf:2120:112"
  "thumbv6m-none-eabi:2652:112"
)

size_of() {
  readelf -SW "$1" | sed 's/^ *\[[ 0-9]*\] *//' | awk '
    function hex(text, value, i, digit) {
      value = 0
      text = tolower(text)
      for (i = 1; i <= length(text); i++) {
        digit = index("0123456789abcdef", substr(text, i, 1)) - 1
        value = value * 16 + digit
      }
      return value
    }
    $1 ~ /^\.(vector_table|text|rodata)$/ { flash += hex($5) }
    $1 == ".data" { flash += hex($5); ram += hex($5) }
    $1 ~ /^\.(bss|uninit)$/ { ram += hex($5) }
    END { print flash + 0, ram + 0 }'
}

out="${CARGO_TARGET_DIR:-target}"
status=0
for entry in "${TARGETS[@]}"; do
  IFS=: read -r target pin_flash pin_ram <<<"$entry"
  cargo build -q --target "$target" --profile chip -p daedalus-chip-blink --bins
  cargo check -q --target "$target" -p daedalus-cpu --no-default-features --features chip
  read -r flash ram < <(size_of "$out/$target/chip/daedalus-chip-blink")
  echo "  $target: $flash B flash, $ram B RAM"
  if ((flash > MAX_FLASH || ram > MAX_RAM)); then
    echo "  $target: over budget ($MAX_FLASH B flash, $MAX_RAM B RAM)" >&2
    status=1
  fi
  if [[ "$flash:$ram" != "$pin_flash:$pin_ram" ]]; then
    echo "  $target: expected $pin_flash:$pin_ram (flash:RAM). If this is intended, update scripts/ci.d/chip.sh and docs/chips.md." >&2
    status=1
  fi
done
exit "$status"
