use daedalus_author_fixture as fixture;
use daedalus_author_fixture::sample::Sample;
use daedalus_macros::node;

#[node(id = "ui.foreign", backend = fixture)]
pub fn f(value: &Sample) -> u32 {
    value.count
}

fn main() {}
