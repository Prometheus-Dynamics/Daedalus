# Daedalus Java Example Project

This folder is a minimal, copyable starting point for authoring Daedalus nodes in **Java** and building a Rust plugin (`.so`/`.dylib`/`.dll`).

## What you get

- `ExampleNodes.java`: nodes defined with Rust-like "decorator" ergonomics using annotations
- `BuildPlugin.java`: builds a plugin library (simple path)
- `EmitManifest.java`: advanced: emits `example_java.manifest.json` (useful for debugging/tests)

## Quickstart (from repo root)

Compile the SDK + this project into a temp directory, then build the plugin:

```bash
OUT=/tmp/daedalus_java_example
mkdir -p "$OUT"

# Compile: SDK + example project
javac -d "$OUT" $(find crates/ffi/lang/java/sdk -name '*.java') $(find crates/ffi/lang/java/examples/example_project -name '*.java')

# Build plugin
java -cp "$OUT" daedalus.example_project.BuildPlugin /tmp/example_java.so
```

## Notes

- Java nodes execute in a subprocess bridge; shaders execute on the Rust GPU side when the manifest includes shader specs.
- For advanced workflows you can still emit a `manifest.json` via `daedalus.example_project.EmitManifest`.
