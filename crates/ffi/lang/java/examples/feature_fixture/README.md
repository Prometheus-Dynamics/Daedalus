# Java: Feature Fixture

Used by `crates/ffi/tests/manifest_feature_suite.rs`.

Covers a broad set of manifest features (defaults, structs/enums, sync policies, raw_io, shaders).

## Run (from repo root)

```bash
OUT=/tmp/daedalus_java_feat
mkdir -p "$OUT"
javac -d "$OUT" $(find crates/ffi/lang/java/sdk -name '*.java') $(find crates/ffi/lang/java/examples -name '*.java')
DAEDALUS_WGSL_DIR=crates/ffi/lang/shaders java -cp "$OUT" daedalus.examples.EmitManifestFeatures /tmp/demo_java_feat.manifest.json
```

