#![allow(unused_imports)]

use daedalus_author_fixture as fixture;
use daedalus_macros::node;

#[node(id = "ui.d", backend = nowhere::at_all)]
pub fn f() -> u32 {
    1
}

fn main() {}
