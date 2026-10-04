//! trybuild UI tests for the `#[node]` and transport macros.
//!
//! These compile a separate trybuild project and are slow, so they are ignored by default and run
//! in their own CI job:
//! `cargo test -p daedalus-rs --features plugins --test transport_macro_ui -- --ignored`.

use trybuild::TestCases;

#[test]
#[ignore = "slow trybuild UI test; run with `--test transport_macro_ui -- --ignored`"]
fn transport_macro_compile_failures() {
    let t = TestCases::new();
    t.pass("tests/ui/transport/ok_transport_plugin.rs");
    t.pass("tests/ui/transport/ok_foreign_type_keys.rs");
    t.compile_fail("tests/ui/transport/fail_adapt_generic.rs");
    t.compile_fail("tests/ui/transport/fail_adapt_unknown_kind.rs");
    t.compile_fail("tests/ui/transport/fail_device_generic.rs");
    t.compile_fail("tests/ui/transport/fail_device_missing_download.rs");
    t.compile_fail("tests/ui/transport/fail_device_mut_input.rs");
    t.compile_fail("tests/ui/transport/fail_plugin_generic.rs");
    t.compile_fail("tests/ui/transport/fail_plugin_foreign_types_syntax.rs");
    t.compile_fail("tests/ui/transport/fail_plugin_missing_id.rs");
    t.compile_fail("tests/ui/transport/fail_type_key_generic.rs");
    t.compile_fail("tests/ui/transport/fail_type_key_on_fn.rs");
}

#[test]
#[ignore = "slow trybuild UI test; run with `--test transport_macro_ui -- --ignored`"]
fn node_macro_compile_failures() {
    let t = TestCases::new();
    t.pass("tests/ui/node/ok_node.rs");
    t.pass("tests/ui/node/ok_id_expressions.rs");
    t.pass("tests/ui/node/ok_three_ref_params.rs");
    t.compile_fail("tests/ui/node/fail_id_not_string.rs");
    t.compile_fail("tests/ui/node/fail_unknown_argument.rs");
    t.compile_fail("tests/ui/node/fail_missing_id.rs");
    t.compile_fail("tests/ui/node/fail_port_ty_and_type_key.rs");
}
