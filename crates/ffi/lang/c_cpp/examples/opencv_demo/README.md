# C/C++ OpenCV Demo (Required)

This example shows a C++ Daedalus plugin node that uses OpenCV `cv::Mat` and `cv::GaussianBlur`.

OpenCV is required:
- C++ headers (`opencv2/*`)
- Linkable OpenCV libraries (via `pkg-config opencv4` or `pkg-config opencv`)

## Build

Build a shared library (single-artifact plugin):

```bash
crates/ffi/lang/c_cpp/examples/opencv_demo/build.sh /tmp/demo_cpp_opencv
```

Optionally also emit a manifest JSON next to it (for the manifest-loader path):

```bash
crates/ffi/lang/c_cpp/examples/opencv_demo/build.sh /tmp/demo_cpp_opencv /tmp/demo_cpp_opencv/demo_cpp_opencv.manifest.json
```

## Node

- `demo_cpp_opencv:blur(img) -> out`
  - `img`/`out` are a small JSON-typed image carrier:
    - `{data_b64,width,height,channels,dtype,layout,encoding}`
    - Prefer `encoding="raw"` (fast). `png` is supported as a fallback.

