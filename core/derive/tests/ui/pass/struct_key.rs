use daedalus_derive::Type;

#[derive(Type)]
#[type_key("acme:thing")]
pub struct Thing {
    pub a: u32,
}

fn main() {}
