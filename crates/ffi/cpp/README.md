# Daedalus FFI C/C++

C and C++ ABI and packaging integration target for Daedalus FFI plugins.

## Target Model

C and C++ plugins should stay on `BackendRuntimeModel::InProcessAbi` when they expose trusted shared
libraries with stable symbols. They should not be routed through the persistent worker pool.

## Trust And Safety

The C/C++ path is an in-process native ABI. It is intended for trusted shared libraries that are
built against the expected Daedalus FFI header and loaded from verified packages. This path is not a
sandbox: native plugin code has the same process permissions as the host, and ABI mismatches can
cause undefined behavior if they are not rejected before registration.

C/C++ package validation should happen before the library is loaded. Runtime loading should then
check required symbols, ABI version metadata, and package integrity before installing handlers into
the host registry.

## Package Shape

C/C++ packages should emit:

- `PluginSchema` for node and port shape
- per-node `BackendConfig` with `backend = c_cpp`, `runtime_model = in_process_abi`,
  `entry_module` pointing at the shared library, and `entry_symbol`
- `PluginPackage` artifacts for shared libraries under `_bundle/native/<platform>/`

Generated schema metadata from C/C++ libraries is future work. Until that lands, C/C++ package
builders must provide explicit package descriptors for node declarations.

## Port Types

The header's `PackageBuilder` types ports from a node's C++ signature when the registration passes
`daedalus::signature<F>()`. Place the macro after the function and use `decltype`, or spell the
function type out:

```cpp
std::int32_t add(std::int32_t a, std::int32_t b) { return a + b; }
DAEDALUS_NODE(add, inputs(a, b), outputs(out), daedalus::signature<decltype(add)>())

DAEDALUS_NODE(scale, inputs(value), outputs(out), daedalus::signature<std::uint32_t(std::uint32_t)>())
```

Parameters type the inputs in order (trailing state or `EventContext` parameters are ignored), a
`std::tuple` return types each output, any other return types the single output, and a
`daedalus::Outputs` return leaves outputs untyped. The `daedalus::TypeExprOf<T>` trait maps:

| C++ | Daedalus |
| --- | --- |
| `bool` | `Bool` |
| `std::int8_t` / `int16_t` / `int32_t` / `int64_t` | `I8` / `I16` / `I32` / `Int` |
| `std::uint8_t` / `uint16_t` / `uint32_t` / `uint64_t` | `U8` / `U16` / `U32` / `U64` |
| `float` / `double` | `F32` / `Float` |
| `std::string`, `std::string_view` | `String` |
| `std::vector<std::uint8_t>`, `BytesView` family, `Rgba8Image` family | `Bytes` |
| `void`, `daedalus::Unit` | `Unit` |
| `std::optional<T>` / `std::vector<T>` / `std::map<K, V>` / `std::tuple<Ts...>` | `Optional` / `List` / `Map` / `Tuple` |
| type with `static constexpr const char* daedalus_type_key` | `Opaque(key)` |

Integers map by size and signedness, so `long long`, `char` and `std::size_t` take the matching
fixed width (`size_t` is `U64` on 64-bit targets). Unmapped types fail descriptor generation;
specialize `daedalus::TypeExprOf<T>` for them. Nodes registered without a signature have no type
information: ports named `payload`, `frame`, `blob` or `rgba8` are `Bytes` and the rest `Int`.

## Current Status

This crate is the home for C/C++ ABI helpers, package APIs, and validation that do not require a
language worker process.
