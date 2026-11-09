#![allow(unused_imports)]

use daedalus_author_fixture as fixture;
use daedalus_macros::node;

#[node(id = "Not Valid", backend = fixture)]
pub fn f() -> u32 {
    1
}

fn main() {}
