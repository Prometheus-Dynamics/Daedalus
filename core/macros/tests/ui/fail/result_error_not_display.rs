#![allow(unused_imports)]

use daedalus_author_fixture as fixture;
use daedalus_macros::node;

pub struct E;

#[node(id = "ui.d", backend = fixture)]
pub fn f() -> Result<u32, E> {
    Ok(1)
}

fn main() {}
