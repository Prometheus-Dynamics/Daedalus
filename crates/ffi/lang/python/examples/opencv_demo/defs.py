from __future__ import annotations

from pathlib import Path

from daedalus_py import CvImage, Plugin, node_rs as node

plugin = Plugin(name="demo_py_opencv", version="0.1.0", description="OpenCV image demo")

RUNTIME_PATH = Path(__file__).with_name("rt.py").resolve().as_posix()


@node(
    plugin=plugin,
    id="demo_py_opencv:blur",
    inputs=("img",),
    outputs=("out",),
    py_path=RUNTIME_PATH,
    py_function="blur",
)
def blur(img: CvImage) -> CvImage:
    # Signature stub:
    # - the decorator captures ports/types/metadata from Python annotations
    # - the actual runtime is in `rt.py` (referenced via `py_path`) so you can keep
    #   “definitions” and “runtime” separate (and keep the runtime dependency-free).
    return img
