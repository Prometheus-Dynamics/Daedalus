#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${1:-/tmp/example_cpp}"
mkdir -p "$OUT_DIR"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../.." && pwd)"

SRC="$ROOT/crates/ffi/lang/c_cpp/examples/example_project/nodes.cpp"
HDR="$ROOT/crates/ffi/lang/c_cpp/sdk/daedalus_c_cpp.h"
SHADERS_DIR="$ROOT/crates/ffi/lang/c_cpp/examples/example_project/shaders"

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
LIB_EXT="so"
if [[ "$OS" == "darwin" ]]; then
  LIB_EXT="dylib"
elif [[ "$OS" == "mingw"* || "$OS" == "msys"* || "$OS" == "cygwin"* ]]; then
  LIB_EXT="dll"
fi

LIB="$OUT_DIR/libexample_cpp_nodes.$LIB_EXT"

echo "[c_cpp] building $LIB"
c++ -std=c++17 -O2 -fPIC -shared -I"$ROOT/crates/ffi/lang/c_cpp/sdk" "$SRC" -o "$LIB"

MANIFEST="$OUT_DIR/example_cpp.manifest.json"
export LIB
export MANIFEST

# Copy shader assets next to the manifest/library so `src_path` resolves.
mkdir -p "$OUT_DIR/shaders"
cp -f "$SHADERS_DIR/"*.wgsl "$OUT_DIR/shaders/"

# Emit a manifest file by calling the dylib's exported `daedalus_cpp_manifest` symbol, then
# patch in cc_path to point back at this dylib (the "manifest file" flow).
python - <<'PY'
import ctypes
import json
import os
from pathlib import Path

lib_path = Path(os.environ["LIB"]).resolve()
out = Path(os.environ["MANIFEST"]).resolve()
out.parent.mkdir(parents=True, exist_ok=True)

class Result(ctypes.Structure):
    _fields_ = [("json", ctypes.c_char_p), ("error", ctypes.c_char_p)]

lib = ctypes.CDLL(str(lib_path))
mf = lib.daedalus_cpp_manifest
mf.restype = Result
free = lib.daedalus_free
free.argtypes = [ctypes.c_void_p]

res = mf()
if res.error:
    err = ctypes.string_at(res.error).decode("utf-8", errors="replace")
    free(res.error)
    raise SystemExit(err)
if not res.json:
    raise SystemExit("daedalus_cpp_manifest returned null")

json_str = ctypes.string_at(res.json).decode("utf-8", errors="replace")
free(res.json)

doc = json.loads(json_str)
doc["language"] = "c_cpp"
for n in doc.get("nodes", []):
    n.setdefault("cc_path", lib_path.name)
    n.setdefault("cc_free", "daedalus_free")

out.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
print(out.as_posix())
PY

echo "[c_cpp] wrote $MANIFEST (and $LIB exports daedalus_cpp_manifest for manifest-less loading)"
