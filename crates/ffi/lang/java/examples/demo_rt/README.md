# Java: Demo Runtime (Roundtrip)

Used by `crates/ffi/tests/manifest_subprocess_roundtrip.rs`.

## Run (from repo root)

Compile the Java SDK + examples, then run the emitter:

```bash
OUT=/tmp/daedalus_java_demo
mkdir -p "$OUT"
javac -d "$OUT" $(find crates/ffi/lang/java/sdk -name '*.java') $(find crates/ffi/lang/java/examples -name '*.java')
java -cp "$OUT" daedalus.examples.EmitManifestDemo /tmp/demo_java_rt.manifest.json
```

