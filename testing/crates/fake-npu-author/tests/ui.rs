//! A node that names a backend path that is not in scope fails to compile, and the error names
//! the path.

#[test]
fn a_backend_path_that_is_not_in_scope_fails_and_is_named() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/ui/model.rs");
    cases.compile_fail("tests/ui/unknown_backend.rs");
    cases.compile_fail("tests/ui/function_body.rs");
}
