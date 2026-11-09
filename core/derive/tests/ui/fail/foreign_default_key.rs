use daedalus_author_fixture::sample::Sample;
use daedalus_derive::Type;

#[derive(Type)]
#[type_key("acme:outer")]
pub struct Outer {
    pub inner: Sample,
}

fn main() {}
