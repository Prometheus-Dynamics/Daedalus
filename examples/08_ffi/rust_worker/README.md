# Rust Worker Example

The Rust plugin written with the in-process authoring model and served out of process. `src/lib.rs`
is the plugin: `#[node]` functions (`add`, `scale` with a `NodeConfig` port, `accumulate` with a
`state(T)` node, `accumulate_scaled` with a state and a config port, `boom` which panics, `describe` which emits an event), declared with
`declare_plugin!`. `src/main.rs` serves it with `daedalus_ffi_rust::Worker`.

## Run

```bash
cargo build -p daedalus-plugins-rust-worker --bin rust_worker
cargo test -p daedalus-plugins-rust-worker --test e2e
```

`tests/e2e.rs` builds the package with `rust_worker_package` for the binary the test built, installs
it with `FfiHost`, and runs a graph of invokes: `add`, then `scale` with the factor, then
`accumulate` twice (the worker keeps the state), then `accumulate` with a state the host sends, a
`describe` whose event comes back, a `boom` that fails its call, and an `add` that still answers.

## Package

`package(executable)` returns the `PluginPackage` for the worker binary at `executable`. Every node
runs in that one persistent worker.

## What Is Not Here

The graph is driven from the host's side with `FfiHost::invoke`. The runtime's graph executor does
not dispatch persistent-worker nodes in this tree, so the graph is not run by the runtime engine.
