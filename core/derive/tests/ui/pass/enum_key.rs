use daedalus_derive::Type;

const KEY: &str = "acme:choice";

#[derive(Type)]
#[type_key(KEY)]
pub enum Choice {
    One,
    Two(u8),
}

fn main() {}
