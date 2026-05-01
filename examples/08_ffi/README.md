# FFI Showcase Examples

These examples define the target source-level shape for complex FFI plugins in every supported
language. Each plugin exposes the same feature surface so SDK changes can be compared against the
native Rust baseline before adding generated package smoke tests.

Current showcase folders:

- `rust/complex_plugin`: native macro baseline.
- `python/complex_plugin`: decorator/dataclass target.
- `node/complex_plugin`: TypeScript builder target.
- `java/complex_plugin`: annotation and package-builder target.
- `cpp/complex_plugin`: header macro and in-process ABI target.
- `all_plugins_giant_graph`: one graph that loads every language plugin and invokes every node
  category.

Each folder includes source, a package build entrypoint, an expected invocation transcript, and a
short note on the current gap from Rust.

Every language complex plugin includes examples for array/dynamic nodes, regular nodes, complex
node IO, GPU nodes, stateful nodes, internal and external adapters, zero-copy payload refs, shared
refs, copy-on-write, mutable access, and owned/move access. The giant graph README and metric
summary describe the combined cross-language proof target.
