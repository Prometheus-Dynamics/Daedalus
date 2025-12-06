# Node.js FFI

Status: supported (manifest + subprocess bridge).

This folder provides a tiny Node.js-side SDK for describing Daedalus nodes and emitting a manifest JSON that the Rust-side loader can execute by spawning `node`.

## Quickstart

From repo root:

```bash
node crates/ffi/lang/node/examples/emit_manifest_demo.mjs
```

This writes `demo_node.manifest.json` alongside the example. The manifest shape matches Rust serde for `TypeExpr` (e.g. `{"Scalar":"Int"}`, `{"Optional":{"Scalar":"Int"}}`) and sets `"language": "node"`.

## Node Function Conventions

- Stateless nodes: export a function that takes positional args and returns either a single value or an array (for multi-output nodes).
- Stateful nodes: export a function that takes a single object argument `{ args, state, state_spec }` and returns:
  - `{ state, outputs }`, or
  - `[state, outputs]`, or
  - just `outputs` (state is preserved).

## TypeScript (tsc-driven)

The Node SDK includes a TypeScript-first pipeline that uses the TypeScript compiler API to:
- discover nodes via decorators (`@nodeMethod(...)` / `@node(...)`)
- emit JS (`tsc`-equivalent emit)
- infer ports and `TypeExpr` types from TS signatures for common cases (scalars, optionals, lists, tuples, structs)
- write a manifest JSON (same schema as JS/Python/Java)

**Example**

`crates/ffi/lang/node/examples/ts_infer/nodes.ts`:

```ts
import { nodeMethod, type Int } from "../../daedalus_node/index.js";

export class DemoTsNodes {
  @nodeMethod({ id: "demo_ts:add" })
  static add(a: Int, b: Int): Int {
    return (Number(a) + Number(b)) as Int;
  }
}
```

Emit a manifest:

```bash
node crates/ffi/lang/node/daedalus_node/tools/emit_manifest_ts.mjs \
  --project crates/ffi/lang/node/examples/ts_infer/tsconfig.json \
  --emit-dir /tmp/daedalus_ts_emit \
  --out /tmp/demo_ts.manifest.json \
  --plugin-name demo_ts
```

End-to-end pack/bundle (TS build → emit → bundle):

```bash
node crates/ffi/lang/node/daedalus_node/tools/pack_ts_project.mjs \
  --project crates/ffi/lang/node/examples/ts_infer/tsconfig.json \
  --plugin-name demo_ts \
  --emit-dir /tmp/daedalus_ts_emit \
  --manifest /tmp/demo_ts.manifest.json \
  --out-name demo_ts_bundle \
  --no-build
```

Or from JS, call `Plugin.packTs(...)` to emit + bundle + generate the Rust wrapper (`cdylib` example).
