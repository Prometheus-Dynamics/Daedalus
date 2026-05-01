# daedalus-data

Portable type and value model for graph metadata, node descriptors, host exchange, and FFI contracts.

## Owns

- `Value`, `ValueRef`, `ValueType`, and `TypeExpr`,
- data descriptors and named type metadata,
- conversion helpers and typed value extraction,
- units and schema-oriented metadata,
- optional JSON, schema, proto, and GPU descriptor helpers.

## Feature Shape

- `json`: JSON conversion support.
- `schema`: JSON Schema export support.
- `proto`: proto3 emission support.
- `gpu`: GPU-facing data descriptors without depending on concrete GPU backends.

Runtime payload ownership and transport adaptation live in `daedalus-transport` and `daedalus-runtime`, not here.

## Registry Ownership

Use owned registries for release-facing hosts, plugins, tests, and multi-tenant processes:

- `TypeRegistry::new()` owns Rust type mappings and type capabilities for one caller.
- `NamedTypeRegistry::new()` owns stable named schemas and host export policies for one caller.

The process-global helpers (`register_type`, `type_expr`, `register_named_type`, `lookup_named_type`, and related snapshot helpers) are convenience APIs for examples, simple binaries, and tooling. Code that already owns an engine or plugin registry should pass that registry through instead of relying on process-global state. Tests and embedders that temporarily use global type helpers can bracket changes with `snapshot_global_registry`, `restore_global_registry`, or `reset_global_registry`.
