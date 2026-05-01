# daedalus-macros

Proc macros for node authoring, plugin wiring, transport declarations, typed data, and GPU metadata.

## Surface

- `node` and `node_handler`: node handler and descriptor generation.
- `plugin`: transport-native plugin module marker.
- `type_key`, `adapt`, and `device`: transport declaration attributes.
- `NodeConfig`: structured config input metadata.
- `GpuBindings` and `GpuStateful`: WGSL/GPU metadata derives.
- `BranchPayload`, `DaedalusTypeExpr`, `ToValue`, and `Outputs`: data and node helper derives.

Macro output should remain deterministic because registry snapshots, UI tests, and generated fixtures depend on stable names and diagnostics.
