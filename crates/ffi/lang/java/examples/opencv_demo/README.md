# Java: OpenCV Demo

Demonstrates running an OpenCV operation (Gaussian blur) inside the **Java subprocess node**.

## Requirements

- OpenCV Java bindings on the classpath (`org.opencv.*`) and the native OpenCV library available for `System.loadLibrary`.

## Run (from repo root)

```bash
OUT=/tmp/daedalus_java_opencv
mkdir -p "$OUT"
# Compile with your OpenCV jar on the classpath (paths are environment-specific).
javac -cp "/path/to/opencv.jar" -d "$OUT" \
  $(find crates/ffi/lang/java/sdk -name '*.java') \
  $(find crates/ffi/lang/java/examples/opencv_demo -name '*.java')

# Run with OpenCV jar + native library available.
java -cp "$OUT:/path/to/opencv.jar" daedalus.examples.EmitManifestOpenCvDemo /tmp/demo_java_opencv.manifest.json
```

## Notes

- The preferred input/output format is `encoding=="raw"` (raw pixel bytes). PNG is supported as a fallback.
