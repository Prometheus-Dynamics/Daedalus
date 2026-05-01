use daedalus::device;

struct GpuI32(i32);

#[device(id = "test.missing_download", cpu = "test:i32", device = "test:i32@gpu")]
fn upload(input: &i32) -> Result<GpuI32, daedalus::transport::TransportError> {
    Ok(GpuI32(*input))
}

fn main() {}
