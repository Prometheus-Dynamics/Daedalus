# Java: Structured Types Demo

Used by `crates/ffi/tests/structured_subprocess_roundtrip.rs`.

## Run (from repo root)

```bash
OUT=/tmp/daedalus_java_struct
mkdir -p "$OUT"
javac -d "$OUT" $(find crates/ffi/lang/java/sdk -name '*.java') $(find crates/ffi/lang/java/examples -name '*.java')
java -cp "$OUT" daedalus.examples.EmitManifestStructuredDemo /tmp/demo_java_struct.manifest.json
```

