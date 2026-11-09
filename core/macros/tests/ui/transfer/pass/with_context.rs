#![allow(unused_imports)]

use daedalus_author_fixture as fixture;
use daedalus_macros::transfer;

pub struct Cx;

#[transfer(from = "a", to = "b", cost = import, backend = fixture)]
pub fn m(value: u8, cx: &Cx) -> u32 {
    let _ = cx;
    value as u32
}

fn main() {}
