# All FFI Plugins Giant Graph

This example is the cross-language proof target. It loads one complex plugin package from each
language showcase and invokes every node category from every package in one graph.

Plugin packages:

- `../rust/complex_plugin/plugin.json`
- `../python/complex_plugin/plugin.json`
- `../node/complex_plugin/plugin.json`
- `../java/complex_plugin/plugin.json`
- `../cpp/complex_plugin/plugin.json`

Required node categories per language:

- `array_dynamic_sum`: array/dynamic node
- `scalar_add`: regular node
- `node_io_complex`: complex node IO
- `gpu_tint`: GPU-resident node
- `accumulate`: stateful node
- `internal_adapter_consume`: adapter-fed internal type conversion
- `external_adapter_consume`: adapter-fed external type conversion
- `zero_copy_len`: zero-copy payload reference
- `shared_ref_len`: shared reference
- `cow_append_marker`: copy-on-write mutation path
- `mutable_brighten`: mutable borrowed payload
- `owned_bytes_len`: owned/move payload

Run the executable proof target from the repository root:

```bash
cargo run -p daedalus-ffi-host --example ffi_all_plugins_giant_graph
```

The example uses the host smoke API to generate package descriptors from the showcase transcripts,
validate bundle artifacts, invoke every language/node-category pair, and print two separate outputs:
structural feature coverage from the example and runtime metrics from the shared FFI telemetry
report.
