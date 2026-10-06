# daedalus-transport

Low-level payload transport contracts shared by registry, planner, runtime, GPU, FFI, and plugin code.

## Owns

- stable `TypeKey` and layout identity,
- payload access modes and residency classes,
- adapter declarations, costs, requirements, and executable adapter tables,
- boundary contracts for dynamic plugin payloads,
- payload envelopes, lifecycle lineage, branch policy, release hooks, and stream pressure policy.
- foreign interfaces (`foreign_interface!`, `ForeignHandle`, `ForeignRef`, `ProvideForeign`):
  `#[repr(C)]` accessor vtables with a key, version and layout hash, so separately built plugins
  read host-owned values zero-copy without sharing their Rust types,
- the standard `daedalus:frame` v2 interface (`FrameView`, `FrameSource`; spec in
  [`docs/foreign-frame-interface.md`](../../docs/foreign-frame-interface.md)).

This crate intentionally does not depend on the runtime, registry, GPU, FFI, or data crates. It defines vocabulary and contracts; higher layers decide when and how to execute them.
