# Standalone Plugin Examples

These are standalone plugin crates. They are kept under `examples/plugins` because they are
copyable project examples, not core workspace packages.

## Included

- `example_project`: native Rust plugin fixture used by dynamic loading and FFI tests.
- `math`: capability-driven arithmetic and utility nodes.
- `framelease`: optional Styx `FrameLease` dynamic plugin example.

Build one with:

```sh
cargo build -p daedalus-plugins-example-project
```
