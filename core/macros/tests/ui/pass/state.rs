use daedalus_author_fixture as fixture;
use daedalus_macros::node;

#[derive(Default)]
pub struct S(u32);

#[node(id = "ui.s", state(S), backend = fixture)]
pub fn f(a: u32, s: &mut S) -> u32 {
    s.0 += a;
    s.0
}

fn main() {}
