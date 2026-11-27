#[test]
fn compile_results() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/fail_*.rs");
}
