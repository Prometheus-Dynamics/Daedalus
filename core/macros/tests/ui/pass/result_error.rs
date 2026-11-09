use daedalus_author_fixture as fixture;
use daedalus_macros::node;

#[node(id = "ui.r", backend = fixture)]
pub fn r(a: u32) -> Result<u32, String> {
    Ok(a)
}

fn main() {}
