use daedalus_author_fixture as fixture;
use daedalus_macros::node;

mod one {
    use super::*;
    #[node(id = "ui.one", backend = fixture)]
    pub fn one() -> u32 {
        1
    }
}

mod two {
    use super::*;
    #[node(id = "ui.two", backend = fixture)]
    pub fn two() -> u32 {
        2
    }
}

pub const NODES: &[&daedalus_registry::NodeRecord] = &[&one::ONE_NODE, &two::TWO_NODE];

fn main() {}
