// Daedalus TypeScript nodes: decorator + inference pattern.
//
// The emitter will:
// - run TS compiler emit (tsc-equivalent)
// - discover decorated methods
// - infer ports/types from your TS signature (for common cases)
// - write a manifest.json pointing js_path at the emitted JS file

import { nodeMethod, type Int } from "../../daedalus_node/index.js";

export class ExampleTsNodes {
  @nodeMethod({ id: "example_ts:add" })
  static add(a: Int, b: Int): Int {
    return (Number(a) + Number(b)) as Int;
  }

  @nodeMethod({ id: "example_ts:split" })
  static split(value: Int): [Int, Int] {
    return [value, (-Number(value) as unknown as Int)];
  }
}

