#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${1:-/tmp/demo_cpp_opencv}"
MANIFEST_OUT="${2:-}"
mkdir -p "$OUT_DIR"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../.." && pwd)"

SRC="$ROOT/crates/ffi/lang/c_cpp/examples/opencv_demo/nodes.cpp"

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
LIB_EXT="so"
if [[ "$OS" == "darwin" ]]; then
  LIB_EXT="dylib"
elif [[ "$OS" == "mingw"* || "$OS" == "msys"* || "$OS" == "cygwin"* ]]; then
  LIB_EXT="dll"
fi

LIB="$OUT_DIR/libdemo_cpp_opencv.$LIB_EXT"

OPENCV_FLAGS="$(pkg-config --cflags --libs opencv4 2>/dev/null || pkg-config --cflags --libs opencv)"

echo "[c_cpp] building $LIB"
c++ -std=c++17 -O2 -fPIC -shared -I"$ROOT/crates/ffi/lang/c_cpp/sdk" $OPENCV_FLAGS "$SRC" -o "$LIB"

if [[ -z "$MANIFEST_OUT" ]]; then
  echo "[c_cpp] built $LIB (exports daedalus_cpp_manifest for manifest-less loading)"
  exit 0
fi

export LIB
export MANIFEST_OUT

python - <<'PY'
import ctypes
import json
import os
from pathlib import Path

lib_path = Path(os.environ["LIB"]).resolve()
out = Path(os.environ["MANIFEST_OUT"]).resolve()
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

echo "[c_cpp] wrote $MANIFEST_OUT"

