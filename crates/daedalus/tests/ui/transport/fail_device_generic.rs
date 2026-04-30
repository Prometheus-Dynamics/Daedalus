use daedalus::device;

struct GpuI32(i32);

fn download(input: &GpuI32) -> Result<i32, daedalus::transport::TransportError> {
    Ok(input.0)
}

#[device(
    id = "test.generic_device",
    cpu = "test:i32",
    device = "test:i32@gpu",
    download = download
)]
fn upload<T>(_input: &T) -> Result<GpuI32, daedalus::transport::TransportError> {
    Ok(GpuI32(0))
}

fn main() {}
