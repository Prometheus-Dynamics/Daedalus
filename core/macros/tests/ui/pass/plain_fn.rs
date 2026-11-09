use daedalus_author_fixture as fixture;
use daedalus_macros::node;

#[node(id = "ui.plain", backend = fixture)]
pub fn plain(a: u32) -> u32 {
    a
}

const _: &daedalus_registry::NodeRecord = &PLAIN_NODE;

fn main() {}
