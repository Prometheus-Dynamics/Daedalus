use daedalus::plugin;

#[plugin(id = "test.generic_plugin")]
pub struct GenericPlugin<T> {
    _marker: std::marker::PhantomData<T>,
}

fn main() {}
