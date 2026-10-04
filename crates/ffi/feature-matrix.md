# FFI Feature Matrix

This matrix tracks how a feature should propagate from the shared contract into each language SDK
and test harness.

Status values:

- `contract`: represented in `daedalus-ffi-core`
- `host`: installed, validated, or executed by `daedalus-ffi-host`
- `sdk-target`: documented target API, implementation still pending
- `test`: covered by generated fixtures or host tests

| Feature | Contract | Host | Rust | Python | Node/TS | Java | C/C++ | Required Tests |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Scalars | contract | host | test | sdk-target | sdk-target | test | test | generated fixture per language, width-exact SDK port types |
| Bytes | contract | host | sdk-target | test | test | test | test | embedded bytes and payload-handle benchmarks |
| Payload refs | contract | host | sdk-target | test | test | test | test | handle validation, lease tracking, SDK transport options |
| Images | contract | host | sdk-target | sdk-target | sdk-target | sdk-target | sdk-target | image feature fixture and validation |
| Structs | contract | host | sdk-target | sdk-target | sdk-target | sdk-target | sdk-target | custom struct round trip |
| Enums | contract | host | sdk-target | sdk-target | sdk-target | sdk-target | sdk-target | enum variant round trip |
| Optional | contract | host | sdk-target | sdk-target | sdk-target | sdk-target | sdk-target | present and absent values |
| Lists | contract | host | sdk-target | sdk-target | sdk-target | sdk-target | sdk-target | homogeneous list conversion |
| Maps | contract | host | sdk-target | sdk-target | sdk-target | sdk-target | sdk-target | string-key map conversion |
| Tuples | contract | host | sdk-target | sdk-target | sdk-target | sdk-target | sdk-target | positional tuple conversion |
| Unit | contract | host | sdk-target | sdk-target | sdk-target | sdk-target | sdk-target | unit input/output conversion |
| Multi-output | contract | host | test | sdk-target | sdk-target | sdk-target | sdk-target | named output matching |
| Stateful nodes | contract | host | test | sdk-target | sdk-target | sdk-target | sdk-target | repeated invoke preserves state |
| Raw events | contract | host | sdk-target | sdk-target | sdk-target | sdk-target | sdk-target | event ordering and metadata |
| Capability nodes | contract | host | test | sdk-target | sdk-target | sdk-target | sdk-target | host capability available/unavailable |
| Custom type keys | contract | host | sdk-target | sdk-target | sdk-target | sdk-target | sdk-target | type key snapshot and serializer lookup |
| Boundary contracts | contract | host | sdk-target | test | test | test | test | access/residency/layout validation |
| Package artifacts | contract | host | sdk-target | sdk-target | sdk-target | test | sdk-target | missing artifact and hash mismatch |
| Worker reuse | contract | host | not applicable | sdk-target | sdk-target | sdk-target | not applicable | repeated invokes do not restart worker |
| Typed errors | contract | host | sdk-target | sdk-target | sdk-target | sdk-target | sdk-target | malformed input and backend failure |

## Scalar Widths

Every SDK declares the exact `ValueType` of a numeric port (`Int` is `i64`, `Float` is `f64`), so
its transport key names one Rust type and the planner only inserts lossless widening adapters. The
wire still carries every integer as `i64` and every float as `f64`; `WireValue::check_type` (used by
`DecodedInvokeResponse::payload_output`) rejects worker outputs that do not fit the port's width.

| Rust | Node/TS | Java | C/C++ |
| --- | --- | --- | --- |
| `i8` `I8` | | `byte`/`Byte` | `std::int8_t` |
| `i16` `I16` | | `short`/`Short` | `std::int16_t` |
| `i32` `I32` | `"i32"` | `int`/`Integer` | `std::int32_t` |
| `i64` `Int` | `"i64"` | `long`/`Long` | `std::int64_t` |
| `u8` `U8` | | `@Scalar("u8")` | `std::uint8_t` |
| `u16` `U16` | | `char`/`Character` | `std::uint16_t` |
| `u32` `U32` | `"u32"` | `@Scalar("u32") long` | `std::uint32_t` |
| `u64` `U64` | `"u64"` | `@Scalar("u64") long` | `std::uint64_t` |
| `f32` `F32` | `"f32"` | `float`/`Float` | `float` |
| `f64` `Float` | `"f64"` | `double`/`Double` | `double` |

## Propagation Checklist

When adding or changing a feature:

1. Add or update the contract type in `daedalus-ffi-core`.
2. Add validation for schema, backend config, package descriptors, and wire values.
3. Add or update the canonical feature spec.
4. Generate the language fixture for Rust, Python, Node/TypeScript, Java, and C/C++.
5. Add package descriptor snapshots for every language.
6. Add host harness assertions for normalized `InvokeResponse` values.
7. Add negative tests for missing fields, malformed worker output, unsupported type shapes, and
   missing package artifacts.
8. Update the SDK authoring docs if the user-facing syntax changes.

## Inference Policy

| Language | Infer By Default | Explicit Declaration Required |
| --- | --- | --- |
| Rust | function signatures, `#[node]`, `#[derive(NodeConfig)]`, state attributes | custom type keys, boundary contracts, package artifacts, native ABI metadata |
| Python | type hints, dataclasses, decorators, memoryview/mmap transport options | custom serializers, boundary contracts, package artifacts |
| Node/TypeScript | builder schemas and callback types, Buffer/shared-memory transport options | custom serializers, boundary contracts, package artifacts |
| Java | annotations, records, method signatures, direct `ByteBuffer`/mmap transport options | port names, config defaults, serializers, state class, classpath/native libs, boundary contracts |
| C/C++ | macros/templates over function signatures, pointer/length ABI metadata | ownership, serializers, state allocation, ABI metadata, boundary contracts |
