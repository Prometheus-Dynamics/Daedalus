use daedalus_author_fixture as fixture;
use daedalus_macros::node;

#[node(id = "ui.g", backend = fixture, generics(T, instance(u32, "u32"), instance(u8, "small")))]
pub fn g<T: Copy>(a: &T) -> T {
    *a
}

const _: &daedalus_registry::NodeRecord = &G_U32_NODE;
const _: &daedalus_registry::NodeRecord = &G_SMALL_NODE;

fn main() {}
