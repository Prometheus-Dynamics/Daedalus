// Runs the wasm smoke module under Node, and prints the graph's sum.
// usage: node scripts/wasm-smoke.mjs path/to/daedalus_wasm_smoke.wasm
import { readFileSync } from "node:fs";

const module = await WebAssembly.compile(readFileSync(process.argv[2]));
const imports = WebAssembly.Module.imports(module);
if (imports.length > 0) {
  throw new Error(`unexpected imports: ${imports.map((i) => `${i.module}.${i.name}`)}`);
}
const { exports } = await WebAssembly.instantiate(module, {});
const result = exports.smoke();
if (result !== 1520) {
  throw new Error(`expected 1520, got ${result}`);
}
console.log(`wasm smoke: ${result}`);
