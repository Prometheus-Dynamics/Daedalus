// Runs the WASI smoke command under Node. It prints the graph's sum.
// usage: node --no-warnings scripts/wasi-smoke.mjs path/to/daedalus-wasi-smoke.wasm
import { readFileSync } from "node:fs";
import { WASI } from "node:wasi";

const wasi = new WASI({ version: "preview1", returnOnExit: true });
const module = await WebAssembly.compile(readFileSync(process.argv[2]));
const instance = await WebAssembly.instantiate(module, wasi.getImportObject());
const code = wasi.start(instance);
if (code !== 0) {
  throw new Error(`the command exited with ${code}`);
}
