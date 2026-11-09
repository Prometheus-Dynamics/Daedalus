use daedalus_cpu_author::Fault;
use daedalus_macros::node;

macro_rules! my_ids {
    ($name:ident) => {
        concat!("example.", stringify!($name))
    };
}

const SCALE_ID: &str = "example.scale";

#[node(id = concat!("example.", "blur"), inputs(value), outputs(out))]
fn blur(value: i32) -> Result<i32, Fault> {
    Ok(value)
}

#[node(id = my_ids!(sharpen), inputs(value), outputs(out))]
fn sharpen(value: i32) -> Result<i32, Fault> {
    Ok(value)
}

#[node(id = SCALE_ID, inputs(value), outputs(out))]
fn scale(value: i32) -> Result<i32, Fault> {
    Ok(value)
}

fn main() {
    assert_eq!(BLUR_DECL.id, "example.blur");
    assert_eq!(SHARPEN_DECL.id, "example.sharpen");
    assert_eq!(SCALE_DECL.id, "example.scale");
}
