# Daedalus FFI Java

Java worker and packaging integration target for Daedalus FFI plugins.

## Target Model

Java plugins use `BackendRuntimeModel::PersistentWorker`: start one JVM, load the
classpath once, negotiate the worker protocol, advertise supported node ids, and dispatch repeated
method calls.

## Package Shape

Java packages should emit or lower into:

- `PluginSchema` for node and port shape
- per-node `BackendConfig` with `backend = java`, `runtime_model = persistent_worker`,
  `entry_class`, `entry_symbol`, `classpath`, `native_library_paths`, and `executable`
- `PluginPackage` artifacts for jars/classes directories under `_bundle/java/`
- native libraries under `_bundle/native/<platform>/`
- optional Maven coordinate and Gradle project metadata for reproducibility

`java_worker_launch` builds the `-cp` and `java.library.path` arguments from `BackendConfig`; package
builders should rewrite classpath and native library paths to bundled paths before launch.

## Port Types

`PackageBuilder` types input ports from the node method's parameters (in order) and a single
output from its return type. A node with several outputs returns a record whose components are
named after them; each component (with its `@Scalar`, if any) types its output, and a return type
that is not such a record fails the build:

```java
public record Split(@Scalar("u64") long high, long low) {}

@Node(id = "split", inputs = {"value"}, outputs = {"high", "low"})
public static Split split(long value) { ... }
```

Ports named
`payload`, `frame`, `blob` or containing `rgba` are `Bytes`. Scalars are width-exact:

| Java | Daedalus |
| --- | --- |
| `boolean`/`Boolean` | `Bool` |
| `byte`/`Byte` | `I8` |
| `short`/`Short` | `I16` |
| `char`/`Character` | `U16` (Java's only unsigned type) |
| `int`/`Integer` | `I32` |
| `long`/`Long` | `Int` (`i64`) |
| `float`/`Float` | `F32` |
| `double`/`Double` | `Float` (`f64`) |
| `String` | `String` |
| `void`/`Void` | `Unit` |

Java has no unsigned integers, so unsigned and pointer-sized ports are declared with
`@Scalar("u8" | "u16" | "u32" | "u64" | "usize" | "isize" | ...)` on the parameter (or on the
method for its single output), e.g. `@Scalar("u32") long count`. The carrier must be an integral
type for integer scalars and `float`/`double` for float scalars, and it must hold the range: use
`long` for `u32`. A `@Scalar("u64") long` holds the value's bits: `Wire.encode(value, "u64")`
writes it as `{"kind":"uint"}` with unsigned semantics (so values above `Long.MAX_VALUE` cross the
wire exactly) and `Wire.decode` returns a `uint` as those bits again; print one with
`Long.toUnsignedString`. Other `Number` types (`BigInteger`, `BigDecimal`, `AtomicLong`, ...) are rejected; any
other class becomes `Opaque` with its `@TypeKey` (or simple class name).

## Wire Values

`Wire.encode`/`Wire.decode` convert between Java values and `WireValue` JSON objects (maps), and
`Wire.write`/`Wire.read` between those and JSON text; `read` keeps integers beyond `long` exact as
`BigInteger`, and `encode` writes a `BigInteger` above `Long.MAX_VALUE` as `uint`.

## Current Status

This crate owns Java packaging helpers for classpath entries, jars/classes directories, Maven/Gradle
metadata, native library metadata, worker launch args, and persistent worker dispatch.
