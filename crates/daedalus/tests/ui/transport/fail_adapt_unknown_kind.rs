use daedalus::adapt;

#[adapt(
    id = "test.bad_kind",
    from = "test:i32",
    to = "test:string",
    kind = "teleport"
)]
fn bad_kind(input: &i32) -> Result<String, daedalus::transport::TransportError> {
    Ok(input.to_string())
}

fn main() {}
