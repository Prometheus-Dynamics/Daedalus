from __future__ import annotations

import base64

import cv2  # type: ignore
import numpy as np


def blur(img):
    # The subprocess bridge passes images as a small struct with base64 bytes.
    # Prefer `encoding=="raw"` (fast, no codec step). PNG is supported only as a fallback.
    #
    # Payload shape (cross-language):
    #   {
    #     "data_b64": "...",
    #     "width": 640, "height": 480, "channels": 4,
    #     "dtype": "u8", "layout": "HWC",
    #     "encoding": "raw" | "png"
    #   }
    #
    # The Rust bridge will upgrade payloads into a helper object with `to_mat()` when possible.
    if hasattr(img, "to_mat"):
        mat = img.to_mat()
    else:
        payload = img
        encoding = str(payload.get("encoding", "raw"))
        buf = base64.b64decode(payload["data_b64"].encode("ascii"))
        if encoding == "png":
            arr = np.frombuffer(buf, dtype=np.uint8)
            mat = cv2.imdecode(arr, cv2.IMREAD_UNCHANGED)
        else:
            dt = np.dtype(str(payload.get("dtype", "u8")))
            w = int(payload["width"])
            h = int(payload["height"])
            ch = int(payload.get("channels", 1))
            arr = np.frombuffer(buf, dtype=dt)
            mat = arr.reshape((h, w)) if ch == 1 else arr.reshape((h, w, ch))

    blurred = cv2.GaussianBlur(mat, (5, 5), 0)

    arr = np.asarray(blurred)
    if not arr.flags["C_CONTIGUOUS"]:
        arr = np.ascontiguousarray(arr)
    h, w = arr.shape[:2]
    ch = 1 if len(arr.shape) == 2 else int(arr.shape[2])
    return {
        "data_b64": base64.b64encode(arr.tobytes(order="C")).decode("ascii"),
        "width": int(w),
        "height": int(h),
        "channels": int(ch),
        "dtype": str(arr.dtype),
        "layout": "HWC",
        "encoding": "raw",
    }
