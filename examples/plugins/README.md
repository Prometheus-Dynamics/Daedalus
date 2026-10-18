# Plugin Examples

Standalone plugin-style crates used as copyable examples and integration fixtures.

## Crates

- `example_project`: native Rust plugin fixture used by plugin and FFI tests.
- `example_project_dylib`: the leaf `cdylib` exporting `example_project`'s plugin
  (`--features dylib`), so crates linking `example_project` never carry its exported symbols.
- `math`: capability-backed arithmetic node examples.
- `foreign_consumer`: a separately built plugin reading host counters through a foreign interface.
- `dependent`: a plugin depending on `example_project`'s plugin (`deps`, linked with
  `export_plugin!(.., deps [..])`).
- `stable_abi`: a plugin whose `cdylib` claims another toolchain, so hosts install and run it
  through the stable handler path; its `reload-v2` / `reload-incompatible` features are the
  rebuilt fixtures of the facade's `dylib_reload` test.

Build one directly with Cargo, for example:

```bash
cargo build -p daedalus-plugins-example-project
```
