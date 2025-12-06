# daedalus-data

Shared type/value model used by node ports, serialization helpers, and capability descriptions.

## Responsibilities
- Define `Value`/`TypeExpr`/`Descriptor` structures used across registry, planner, and runtime.
- Provide JSON/proto/schema conversion helpers.
- Utilities for units/metadata and GPU-friendly payloads (feature-gated).

## Features
- `json`: JSON codec and base64 helpers (default).
- `proto`: protobuf conversion helpers.
- `schema`: schema export utilities.
- `gpu`: GPU-specific descriptors for planner/runtime interoperability.

## Usage
- Use `TypeExpr`/`Value` to describe ports in node descriptors.
- Use conversion helpers to serialize/deserialize payload metadata.
- Feature-gate optional codecs to keep dependencies lean.
