use daedalus::adapt;

#[adapt(id = "test.generic", from = "test:value", to = "test:value")]
fn generic_adapter<T: Clone>(input: &T) -> Result<T, daedalus::transport::TransportError> {
    Ok(input.clone())
}

fn main() {}
