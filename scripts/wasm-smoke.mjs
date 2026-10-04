// Runs the wasm smoke module (examples/wasm_smoke) under Node.
// usage: node scripts/wasm-smoke.mjs path/to/daedalus_wasm_smoke.wasm
import { readFileSync } from "node:fs";

const module = await WebAssembly.compile(readFileSync(process.argv[2]));
const imports = WebAssembly.Module.imports(module);
if (imports.length > 0) {
  throw new Error(`unexpected imports: ${imports.map((i) => `${i.module}.${i.name}`)}`);
}
const { exports } = await WebAssembly.instantiate(module, {});
const failed = exports.smoke();
if (failed !== 0) {
  throw new Error(`runtime mode #${failed} produced a wrong result`);
}
console.log("wasm smoke: serial, parallel and adaptive runs ok");
