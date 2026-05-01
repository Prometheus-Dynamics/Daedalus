from __future__ import annotations

import sys
from pathlib import Path

# Compatibility shim for older local imports. The actual Python SDK package lives in
# `crates/ffi/python/sdk/daedalus_ffi`.
sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "crates/ffi/python/sdk"))

from daedalus_ffi import *  # noqa: F401,F403
