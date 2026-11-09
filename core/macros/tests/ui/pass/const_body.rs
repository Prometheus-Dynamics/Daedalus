use daedalus_author_fixture as fixture;
use daedalus_macros::node;

#[node(id = "ui.const", inputs(a), outputs(b), backend = fixture(a: u8, b: u8))]
pub const BODY: &str = "text";

const _: &daedalus_registry::NodeRecord = &BODY_NODE;

fn main() {}
