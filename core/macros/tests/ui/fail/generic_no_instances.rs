#![allow(unused_imports)]

use daedalus_author_fixture as fixture;
use daedalus_macros::node;

#[node(id = "ui.d", backend = fixture)]
pub fn f<T: Copy>(a: &T) -> T {
    *a
}

fn main() {}
