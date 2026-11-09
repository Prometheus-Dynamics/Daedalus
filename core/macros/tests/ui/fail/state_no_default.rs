#![allow(unused_imports)]

use daedalus_author_fixture as fixture;
use daedalus_macros::node;

pub struct S;

#[node(id = "ui.d", state(S), backend = fixture)]
pub fn f(s: &mut S) -> u32 {
    let _ = s;
    1
}

fn main() {}
