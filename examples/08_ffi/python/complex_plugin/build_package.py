import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "crates/ffi/python/sdk"))

from ffi_showcase import showcase_plugin

showcase_plugin.write(Path(__file__).with_name("plugin.json"))
