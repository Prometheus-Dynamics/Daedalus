# Daedalus Java (Manifest FFI)

This repo’s Java support is **manifest-based**: Java nodes are executed via a small Java subprocess bridge (`crates/ffi/src/java.rs`) using `java_classpath` + `java_class` + `java_method` fields on each node entry.

## What’s in here

- `sdk/daedalus/bridge/*`: Optional helper types that unlock better parity:
  - `daedalus.bridge.RawIo` + `daedalus.bridge.Extra`: enable `raw_io` nodes to push multiple outputs/events per tick.
  - `daedalus.bridge.StatefulInvocation` + `daedalus.bridge.StateResult`: nicer stateful node ergonomics (no reliance on Java parameter names).
- `sdk/daedalus/manifest/*`: tiny “builder” helpers to emit `manifest.json` with Rust-compatible shapes (`TypeExpr`, shader specs, sync groups, etc).
- `examples/*`: Java fixtures used by the Rust test suite to validate round-trips (feature suite + demo + shader nodes).

## Running the Java examples

The Rust tests compile and run the Java examples on the fly using `javac` + `java`.

Manually, you can do the same (from repo root):

```bash
OUT=/tmp/daedalus_java_demo
mkdir -p "$OUT"
javac -d "$OUT" $(find crates/ffi/lang/java/sdk crates/ffi/lang/java/examples -name '*.java')
java -cp "$OUT" daedalus.examples.EmitManifestDemo "$OUT/demo_java.manifest.json"
```

