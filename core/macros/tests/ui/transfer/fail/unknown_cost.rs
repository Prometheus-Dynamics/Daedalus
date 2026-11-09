#![allow(unused_imports)]

use daedalus_author_fixture as fixture;
use daedalus_macros::transfer;

#[transfer(from = "a", to = "b", cost = free, backend = fixture)]
pub fn m(value: u8) -> u8 {
    value
}

fn main() {}
