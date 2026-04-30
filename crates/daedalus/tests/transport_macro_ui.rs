use trybuild::TestCases;

#[test]
fn transport_macro_compile_failures() {
    let t = TestCases::new();
    t.pass("tests/ui/transport/ok_transport_plugin.rs");
    t.compile_fail("tests/ui/transport/fail_adapt_generic.rs");
    t.compile_fail("tests/ui/transport/fail_adapt_unknown_kind.rs");
    t.compile_fail("tests/ui/transport/fail_device_generic.rs");
    t.compile_fail("tests/ui/transport/fail_device_missing_download.rs");
    t.compile_fail("tests/ui/transport/fail_device_mut_input.rs");
    t.compile_fail("tests/ui/transport/fail_plugin_generic.rs");
    t.compile_fail("tests/ui/transport/fail_plugin_missing_id.rs");
    t.compile_fail("tests/ui/transport/fail_type_key_generic.rs");
    t.compile_fail("tests/ui/transport/fail_type_key_on_fn.rs");
}
