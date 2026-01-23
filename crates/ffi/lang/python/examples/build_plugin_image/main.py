from __future__ import annotations

import sys
from pathlib import Path
from functools import partial

# Allow running directly from repo root.
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import cv2  # type: ignore

from daedalus_py import Plugin, CvImage, node  # noqa: E402

plugin = Plugin(
    name="demo_py_image",
    version="0.1.1",
    description="Image roundtrip via OpenCV",
    metadata={"author": "example"},
)
node = partial(node, plugin=plugin)


@node(
    id="demo_py_image:blur",
    label="Blur",
    metadata={"category": "image"},
)
def blur(img: CvImage) -> CvImage:
    if isinstance(img, dict):
        img = CvImage.from_payload(img)
    mat = img.to_mat()
    out = cv2.GaussianBlur(mat, (5, 5), 0)
    h, w = out.shape[:2]

    # Mark the frame so multi-language pipelines can visually confirm each stage ran.
    center = (max(1, w // 5), max(1, h // 5))
    radius = max(5, min(w, h) // 10)
    color = (0, 255, 0, 255) if (len(out.shape) == 3 and out.shape[2] == 4) else (0, 255, 0)
    cv2.circle(out, center, radius, color, thickness=3)
    cv2.putText(
        out,
        "PY",
        (10, max(20, h // 10)),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.9,
        color,
        2,
        cv2.LINE_AA,
    )
    return CvImage.from_mat(out)


if __name__ == "__main__":
    artifact = plugin.build(out_name="demo_py_image", bundle=True)
    print(f"Built plugin to {artifact}")
