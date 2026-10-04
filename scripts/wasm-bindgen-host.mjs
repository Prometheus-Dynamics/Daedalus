// Drives the wasm-bindgen host example (examples/wasm_bindgen_host) under Node.
// usage: node scripts/wasm-bindgen-host.mjs path/to/daedalus_wasm_bindgen_host.js
// (the output of `wasm-bindgen --target nodejs`).
import { createRequire } from "node:module";
import { resolve } from "node:path";

// The module's engine Clock reads `performance.now()`: a simulated one that advances 1 ms per
// reading makes every tick's graph duration a whole, non-zero number of milliseconds.
let nowMs = 0;
performance.now = () => (nowMs += 1);

const { Pipeline } = createRequire(import.meta.url)(resolve(process.argv[2]));
const pipeline = new Pipeline(2, 1);
for (let x = 0; x < 32; x += 1) {
  if (!pipeline.push(x)) throw new Error(`push(${x}) was refused`);
  const ms = pipeline.tick();
  if (!(ms >= 1 && Math.abs(ms - Math.round(ms)) < 1e-6)) {
    throw new Error(`tick ${x}: graph duration ${ms} ms is not on the injected clock`);
  }
  const y = pipeline.take();
  if (y !== 2 * x + 1) throw new Error(`tick ${x}: expected ${2 * x + 1}, got ${y}`);
}
if (pipeline.take() !== undefined) throw new Error("output left over after the last take");
pipeline.free();
console.log("wasm-bindgen host: 32 ticks driven from JS on the performance.now() clock");
