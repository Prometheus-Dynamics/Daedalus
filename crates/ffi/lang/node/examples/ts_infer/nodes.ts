import { nodeMethod, type Int } from "../../daedalus_node/index.js";

export class DemoTsNodes {
  @nodeMethod({ id: "demo_ts:add" })
  static add(a: Int, b: Int): Int {
    return (Number(a) + Number(b)) as Int;
  }

  @nodeMethod({ id: "demo_ts:split" })
  static split(value: Int): [Int, Int] {
    return [value, (-Number(value) as unknown as Int)];
  }

  @nodeMethod({ id: "demo_ts:map_len" })
  static map_len(m: Record<string, Int>): Int {
    return Object.keys(m).length as unknown as Int;
  }
}
