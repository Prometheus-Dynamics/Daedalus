# FFI SDK Authoring Targets

This document defines the intended source-level authoring shape for foreign-language plugins before
the showcase examples are written. The Rust plugin macros are the baseline. Other SDKs should match
the same concepts with language-native syntax instead of making users hand-write schema JSON.

## Baseline Plugin

Every language showcase should express the same plugin surface:

- scalar node
- multi-output node
- config/defaulted input node
- stateful node
- metadata/default-source port node
- capability-backed node
- custom struct, enum, optional, list, map, tuple, and unit values
- bytes payload node
- image payload node
- raw event emission
- typed failure diagnostics
- custom type key and boundary contract declarations
- package emission to `plugin.json`

## Rust Baseline

Rust is the reference authoring model because the macro system already owns typed ports, config
ports, state, capabilities, and plugin declaration.

```rust
use daedalus::macros::{NodeConfig, node};
use daedalus::{PluginRegistry, declare_plugin, runtime::NodeError};

#[derive(Clone, Debug, NodeConfig)]
struct ScaleConfig {
    #[port(default = 2, min = 1, max = 16, policy = "clamp")]
    factor: i32,
}

#[derive(Default)]
struct AccumState {
    sum: i32,
}

#[node(id = "add", inputs("a", "b"), outputs("out"))]
fn add(a: i32, b: i32) -> Result<i32, NodeError> {
    Ok(a + b)
}

#[node(id = "split", inputs("value"), outputs("positive", "negative"))]
fn split(value: i32) -> Result<(i32, i32), NodeError> {
    Ok((value, -value))
}

#[node(id = "scale", inputs("value", config = ScaleConfig), outputs("out"))]
fn scale(value: i32, cfg: ScaleConfig) -> Result<i32, NodeError> {
    Ok(value * cfg.factor)
}

#[node(id = "accum", inputs("value"), outputs("sum"), state(AccumState))]
fn accum(value: i32, state: &mut AccumState) -> Result<i32, NodeError> {
    state.sum += value;
    Ok(state.sum)
}

#[node(id = "cap_add", capability = "Add", inputs("a", "b"), outputs("out"))]
fn cap_add<T>(a: T, b: T) -> Result<T, NodeError>
where
    T: std::ops::Add<Output = T> + Clone + Send + Sync + 'static,
{
    Ok(a + b)
}

fn install(registry: &mut PluginRegistry) {
    registry.register_capability_typed::<i32, _>("Add", |a, b| Ok(*a + *b));
}

declare_plugin!(ShowcasePlugin, "ffi_showcase", [add, split, scale, accum, cap_add], install = install);
```

Rust can infer most schema data from signatures and attributes. Custom type keys, boundary
contracts, and package artifacts still need explicit declarations because they are host-contract
metadata, not function signature metadata.

## Python Target

Python should use decorators and dataclasses. Type hints should provide the common path; explicit
schema helpers cover payload handles, custom type keys, and boundary contracts.

```python
from dataclasses import dataclass
from daedalus import Config, State, image, node, plugin, type_key

@dataclass
class ScaleConfig(Config):
    factor: int = Config.port(default=2, min=1, max=16, policy="clamp")

@dataclass
class AccumState(State):
    sum: int = 0

@type_key("example.Point")
@dataclass
class Point:
    x: float
    y: float

@node(id="add", inputs=["a", "b"], outputs=["out"])
def add(a: int, b: int) -> int:
    return a + b

@node(id="split", inputs=["value"], outputs=["positive", "negative"])
def split(value: int) -> tuple[int, int]:
    return value, -value

@node(id="scale", inputs=["value", ScaleConfig], outputs=["out"])
def scale(value: int, cfg: ScaleConfig) -> int:
    return value * cfg.factor

@node(id="accum", inputs=["value"], outputs=["sum"], state=AccumState)
def accum(value: int, state: AccumState) -> int:
    state.sum += value
    return state.sum

@node(id="boost_image", inputs=["frame"], outputs=["frame"])
def boost_image(frame: image.Rgba8) -> image.Rgba8:
    return frame.map_pixels(lambda r, g, b, a: (min(r + 8, 255), g, b, a))

plugin("ffi_showcase", nodes=[add, split, scale, accum, boost_image]).write("plugin.json")
```

Inference target:

- infer scalar, optional, list, map, tuple, dataclass struct, and enum shapes from type hints
- infer node ids and port names from decorators
- require explicit declarations for custom serializers, raw payload handles, boundary contracts, and
  package artifacts

## Node/TypeScript Target

TypeScript should be the primary Node authoring surface. A builder API is more predictable than
runtime decorators because emitted metadata and TypeScript compiler behavior are easier to test.

```ts
import { config, image, node, plugin, state, typeKey } from "@daedalus/plugin";

const ScaleConfig = config("ScaleConfig", {
  factor: config.port("i32", { default: 2, min: 1, max: 16, policy: "clamp" }),
});

const AccumState = state("AccumState", { sum: "i32" }, () => ({ sum: 0 }));

const Point = typeKey("example.Point", {
  x: "f64",
  y: "f64",
});

const add = node("add")
  .inputs({ a: "i32", b: "i32" })
  .outputs({ out: "i32" })
  .run(({ a, b }) => ({ out: a + b }));

const split = node("split")
  .inputs({ value: "i32" })
  .outputs({ positive: "i32", negative: "i32" })
  .run(({ value }) => ({ positive: value, negative: -value }));

const accum = node("accum")
  .state(AccumState)
  .inputs({ value: "i32" })
  .outputs({ sum: "i32" })
  .run(({ value }, state) => {
    state.sum += value;
    return { sum: state.sum };
  });

const boostImage = node("boost_image")
  .inputs({ frame: image.rgba8() })
  .outputs({ frame: image.rgba8() })
  .run(({ frame }) => ({ frame }));

await plugin("ffi_showcase", [add, split, accum, boostImage]).write("plugin.json");
```

Inference target:

- infer callback argument/result TypeScript types from explicit builder schemas
- allow decorators later, but keep the builder as the stable lowest-common-denominator API
- require explicit declarations for custom type keys, boundary contracts, package artifacts, and
  payload handle transport

## Java Target

Java should use annotations for node metadata and explicit package builder APIs for classpath,
native libraries, and package artifacts.

```java
@DaedalusPlugin(id = "ffi_showcase")
public final class ShowcasePlugin {
  public static final class AccumState {
    public long sum;
  }

  public record ScaleConfig(@Input(defaultInt = 2, min = 1, max = 16, policy = "clamp") int factor) {}

  @TypeKey("example.Point")
  public record Point(double x, double y) {}

  @Node(id = "add", inputs = {"a", "b"}, outputs = {"out"})
  public static long add(long a, long b) {
    return a + b;
  }

  @Node(id = "split", inputs = {"value"}, outputs = {"positive", "negative"})
  public static Outputs split(long value) {
    return Outputs.of("positive", value, "negative", -value);
  }

  @Node(id = "scale", inputs = {"value", "config"}, outputs = {"out"})
  public static long scale(long value, ScaleConfig config) {
    return value * config.factor();
  }

  @Node(id = "accum", inputs = {"value"}, outputs = {"sum"}, state = AccumState.class)
  public static long accum(long value, AccumState state) {
    state.sum += value;
    return state.sum;
  }
}
```

Inference target:

- infer scalar, record struct, enum, optional, list, map, tuple-equivalent, and unit-like values
  from reflection/annotation processing
- require explicit annotations for port names, config defaults, custom serializers, state class,
  type keys, boundary contracts, package artifacts, classpath, and native library metadata

## C/C++ Target

C/C++ should use a small header API. The macro layer should emit schema descriptors and ABI metadata
while keeping native function bodies ordinary C++.

```cpp
#include <daedalus.hpp>

struct ScaleConfig {
  int32_t factor = 2;
};

struct AccumState {
  int64_t sum = 0;
};

DAEDALUS_TYPE_KEY(Point, "example.Point")
struct Point {
  double x;
  double y;
};

DAEDALUS_NODE(add, inputs(a, b), outputs(out))
int64_t add_i64(int64_t a, int64_t b) {
  return a + b;
}

DAEDALUS_NODE(split, inputs(value), outputs(positive, negative))
daedalus::Outputs split_i64(int64_t value) {
  return daedalus::outputs("positive", value, "negative", -value);
}

DAEDALUS_STATEFUL_NODE(accum, AccumState, inputs(value), outputs(sum))
int64_t accum_i64(int64_t value, AccumState& state) {
  state.sum += value;
  return state.sum;
}

DAEDALUS_PLUGIN(ffi_showcase, add_i64, split_i64, accum_i64);
```

Inference target:

- infer simple scalar ABI shapes and registered structs from macros/templates
- require explicit declarations for ownership, pointer/length payloads, state allocation,
  serializers, boundary contracts, package artifacts, and ABI version metadata

## Cross-Language Rules

- Node ids, port names, type keys, boundary contracts, and package artifact paths must be stable
  across languages for the same showcase feature.
- Every SDK must emit `PluginSchema`, `BackendConfig`, and `PluginPackage`; users should not
  hand-write those except for debugging.
- Every SDK must expose an escape hatch for explicit schema declarations when inference is unclear.
- Payload-heavy features should prefer payload handles or package artifacts over embedded byte
  arrays on hot paths.
- Worker runtimes must keep stdout reserved for protocol frames and put diagnostics in events or
  stderr.
