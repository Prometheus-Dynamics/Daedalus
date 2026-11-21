#!/usr/bin/env bash
# Builds the Khronos validation layer at the tag and commit scripts/vvl.pin names, for x86_64 (the
# gate's lavapipe steps vvl and vvl-wgpu, scripts/vvl-run.py) or aarch64 (the board's batch,
# VVL_DIR). Both are built with zig against glibc 2.36, the board's, so the two layers come from
# one recipe.
#
#   scripts/vvl-build.sh --arch x86_64|aarch64 --out DIR [--work DIR] [--src DIR]
#
# --out gets libVkLayer_khronos_validation.so, its manifest (library_path ./), SHA256SUMS and
# BUILD.txt. --work holds the sources, their dependencies and the build (several GiB; on NVMe, never
# the HDD); it defaults to DIR/../vvl-work. --src is an existing clone of the layer at the pinned
# commit, used instead of cloning. The dependencies come at the versions the layer's own
# scripts/known_good.json names.
#
# The layer is linked with -z nodelete: zig's static libc++ registers a destructor that otherwise
# runs after the loader has unloaded the layer, and every process exits 139 (E13, 2026-11-20).
#
# A build's hash depends on the work directory and the zig version (paths end up in the library).
# The gate checks the layer it runs against the hash in scripts/vvl.pin, so after a rebuild that
# changes it, record the new hash there.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
repo=$PWD
# shellcheck source=vvl.pin
source scripts/vvl.pin

arch="" out="" work="" src=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --arch) arch="$2"; shift 2 ;;
    --out) out="$2"; shift 2 ;;
    --work) work="$2"; shift 2 ;;
    --src) src="$2"; shift 2 ;;
    *) echo "vvl-build: unknown argument $1" >&2; exit 2 ;;
  esac
done
case "$arch" in
  x86_64 | aarch64) ;;
  *) echo "vvl-build: --arch x86_64 or aarch64" >&2; exit 2 ;;
esac
[[ -n "$out" ]] || { echo "vvl-build: --out DIR" >&2; exit 2; }
mkdir -p "$out"
out=$(cd "$out" && pwd)
work=${work:-$out/../vvl-work}
mkdir -p "$work"
work=$(cd "$work" && pwd)
jobs=${VVL_JOBS:-8}

if [[ -z "$src" ]]; then
  src="$work/src"
  if [[ ! -d "$src/.git" ]]; then
    git clone --filter=blob:none --branch "$VVL_TAG" "$VVL_REPO" "$src"
  fi
fi
have=$(git -C "$src" rev-parse HEAD)
if [[ "$have" != "$VVL_COMMIT" ]]; then
  echo "vvl-build: $src is at $have, not the pinned $VVL_COMMIT ($VVL_TAG)" >&2
  exit 1
fi

# zig as the compiler: its linker takes --version-script as two arguments.
tools="$work/tools-$arch"
mkdir -p "$tools"
target="$arch-linux-gnu.2.36"
printf '#!/bin/sh\nexec zig cc -target %s "$@"\n' "$target" > "$tools/cc"
cat > "$tools/cxx" <<EOF
#!/bin/bash
args=()
for a in "\$@"; do
  case "\$a" in
    --version-script=*) args+=("--version-script" "-Xlinker" "\${a#--version-script=}") ;;
    *) args+=("\$a") ;;
  esac
done
exec zig c++ -target $target "\${args[@]}"
EOF
printf '#!/bin/sh\nexec zig ar "$@"\n' > "$tools/ar"
printf '#!/bin/sh\nexec zig ranlib "$@"\n' > "$tools/ranlib"
chmod +x "$tools/cc" "$tools/cxx" "$tools/ar" "$tools/ranlib"
cat > "$tools/toolchain.cmake" <<EOF
set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR $arch)
set(CMAKE_C_COMPILER $tools/cc)
set(CMAKE_CXX_COMPILER $tools/cxx)
set(CMAKE_AR $tools/ar)
set(CMAKE_RANLIB $tools/ranlib)
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
EOF
export ZIG_GLOBAL_CACHE_DIR="$work/zig-cache" ZIG_LOCAL_CACHE_DIR="$work/zig-cache"

external="$work/external-$arch" build="$work/build-$arch" install="$work/install-$arch"
python3 -I "$src/scripts/update_deps.py" --skip-existing-install --dir "$external" --config Release \
  --generator Ninja --optional tests \
  --cmake_var "CMAKE_TOOLCHAIN_FILE=$tools/toolchain.cmake" --cmake_var CMAKE_BUILD_TYPE=Release
cmake -S "$src" -B "$build" -G Ninja -C "$external/helper.cmake" \
  -DCMAKE_TOOLCHAIN_FILE="$tools/toolchain.cmake" -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTS=OFF -DBUILD_WERROR=OFF \
  -DBUILD_WSI_XCB_SUPPORT=OFF -DBUILD_WSI_XLIB_SUPPORT=OFF -DBUILD_WSI_WAYLAND_SUPPORT=OFF \
  -DCMAKE_MODULE_LINKER_FLAGS="-Wl,-z,nodelete" -DCMAKE_SHARED_LINKER_FLAGS="-Wl,-z,nodelete" \
  -DCMAKE_INSTALL_PREFIX="$install"
cmake --build "$build" -j "$jobs"
cmake --install "$build"

lib="$install/lib/libVkLayer_khronos_validation.so"
manifest="$install/share/vulkan/explicit_layer.d/VkLayer_khronos_validation.json"
case "$arch" in
  x86_64) strip_with=strip ;;
  aarch64) strip_with=aarch64-linux-gnu-strip ;;
esac
command -v "$strip_with" > /dev/null || strip_with=llvm-strip
"$strip_with" -o "$out/libVkLayer_khronos_validation.so" "$lib"
sed 's#"library_path": *"[^"]*"#"library_path": "./libVkLayer_khronos_validation.so"#' \
  "$manifest" > "$out/VkLayer_khronos_validation.json"
(cd "$out" && sha256sum libVkLayer_khronos_validation.so VkLayer_khronos_validation.json > SHA256SUMS)
cat > "$out/BUILD.txt" <<EOF
Khronos Vulkan-ValidationLayers $VVL_TAG, commit $VVL_COMMIT ($VVL_REPO), dependencies at its
scripts/known_good.json. Built by $repo/scripts/vvl-build.sh with zig $(zig version), target
$target, Release, BUILD_TESTS=OFF, no WSI, linked with -z nodelete, stripped with $strip_with.
EOF
echo "vvl-build: $out"
cat "$out/SHA256SUMS"
