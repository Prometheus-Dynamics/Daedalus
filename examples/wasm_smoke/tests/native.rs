//! The smoke graph natively, so a failure shows up without a WebAssembly toolchain.

#[test]
fn the_sum_is_right() {
    assert_eq!(daedalus_wasm_smoke::sum(), 1520);
}
