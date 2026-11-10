#[test]
fn compile_results() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/ui/ok_*.rs");
    cases.compile_fail("tests/ui/fail_*.rs");
}
