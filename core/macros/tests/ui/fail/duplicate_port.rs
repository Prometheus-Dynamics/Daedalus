#![allow(unused_imports)]

use daedalus_author_fixture as fixture;
use daedalus_macros::node;

#[node(id = "ui.d", inputs(same, same), backend = fixture)]
pub fn f(a: u32, b: u32) -> u32 {
    a + b
}

fn main() {}
