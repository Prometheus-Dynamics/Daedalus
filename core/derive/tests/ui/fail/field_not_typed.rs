use daedalus_derive::Type;

pub struct NotTyped;

#[derive(Type)]
#[type_key("acme:outer")]
pub struct Outer {
    pub inner: NotTyped,
}

fn main() {}
