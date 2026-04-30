use daedalus::device;
use daedalus::transport::TransportError;

struct GpuI32(i32);

fn download(input: &GpuI32) -> Result<i32, TransportError> {
    Ok(input.0)
}

#[device(
    id = "test.bad_device",
    cpu = "test:i32",
    device = "test:i32@gpu",
    download = download
)]
fn upload(input: &mut i32) -> Result<GpuI32, TransportError> {
    Ok(GpuI32(*input))
}

fn main() {}
