//! Prints the smoke graph's sum, under WASI.

fn main() {
    println!("{}", daedalus_wasm_smoke::sum());
}
