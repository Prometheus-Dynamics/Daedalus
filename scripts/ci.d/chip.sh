#!/usr/bin/env bash
# Builds the blink firmware in its three modes for each chip target and checks their sizes.
#
# Compiled size is pinned exactly, so any change has to update this file with a reason. The pins
# are "flash:ram" in bytes, from `.vector_table + .text + .rodata + .data` and
# `.data + .bss + .uninit`, and are in docs/chips.md with the other modes' figures.
set -euo pipefail

readonly MAX_FLASH=12288
readonly MAX_RAM=584
readonly TUNABLE_MAX_FLASH=6144
readonly TUNABLE_MAX_RAM=512
# Above the 12 KiB the D7 brief asks for. docs/chips.md says what the space is.
readonly LOADED_MAX_FLASH=17920
readonly LOADED_MAX_RAM=584
readonly TARGETS=(
  "thumbv7em-none-eabihf:2120:112"
  "thumbv6m-none-eabi:2664:112"
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

# Prints a firmware's size and fails if it is over `max_flash` or `max_ram`.
check_budget() {
  local target=$1 name=$2 max_flash=$3 max_ram=$4 flash ram
  read -r flash ram < <(size_of "$out/$target/chip/$name")
  echo "  $target $name: $flash B flash, $ram B RAM"
  if ((flash > max_flash || ram > max_ram)); then
    echo "  $target $name: over budget ($max_flash B flash, $max_ram B RAM)" >&2
    status=1
  fi
}

out="${CARGO_TARGET_DIR:-target}"
status=0
for entry in "${TARGETS[@]}"; do
  IFS=: read -r target pin_flash pin_ram <<<"$entry"
  cargo build -q --target "$target" --profile chip -p daedalus-chip-blink --bins
  cargo check -q --target "$target" -p daedalus-cpu --no-default-features --features chip
  check_budget "$target" daedalus-chip-blink "$MAX_FLASH" "$MAX_RAM"
  check_budget "$target" daedalus-chip-blink-tunable "$TUNABLE_MAX_FLASH" "$TUNABLE_MAX_RAM"
  check_budget "$target" daedalus-chip-blink-loaded "$LOADED_MAX_FLASH" "$LOADED_MAX_RAM"
  read -r flash ram < <(size_of "$out/$target/chip/daedalus-chip-blink")
  if [[ "$flash:$ram" != "$pin_flash:$pin_ram" ]]; then
    echo "  $target: expected $pin_flash:$pin_ram (flash:RAM). If this is intended, update scripts/ci.d/chip.sh and docs/chips.md." >&2
    status=1
  fi
done
exit "$status"
