use daedalus_author_fixture as fixture;
use daedalus_macros::node;

#[node(id = "ui.f", fire = "any", reusable, backend = fixture)]
pub fn f(a: u32) -> u32 {
    a
}

fn main() {}
