use daedalus::type_key;

#[type_key("test:generic")]
struct Generic<T> {
    value: T,
}

fn main() {}
