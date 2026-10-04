// Runs the WASI smoke command (examples/wasm_smoke, bin `daedalus-wasi-smoke`) under Node's WASI.
// usage: node --no-warnings scripts/wasi-smoke.mjs path/to/daedalus-wasi-smoke.wasm
import { readFileSync } from "node:fs";
import { WASI } from "node:wasi";

const wasi = new WASI({ version: "preview1", returnOnExit: true });
const module = await WebAssembly.compile(readFileSync(process.argv[2]));
const instance = await WebAssembly.instantiate(module, wasi.getImportObject());
const failed = wasi.start(instance);
if (failed !== 0) {
  throw new Error(`runtime mode #${failed} produced a wrong result`);
}
console.log("wasi smoke: serial, parallel and adaptive runs ok on the platform clock");
