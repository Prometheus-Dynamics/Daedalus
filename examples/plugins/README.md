# Plugin Examples

Standalone plugin-style crates used as copyable examples and integration fixtures.

## Crates

- `example_project`: native Rust plugin fixture used by plugin and FFI tests.
- `math`: capability-backed arithmetic node examples.
- `framelease`: optional Styx frame lease plugin example.
- `foreign_consumer`: a separately built plugin reading host counters through a foreign interface.
- `dependent`: a plugin depending on `example_project`'s plugin (`deps`, linked with
  `export_plugin!(.., deps [..])`).

Build one directly with Cargo, for example:

```bash
cargo build -p daedalus-plugins-example-project
```
