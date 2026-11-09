use daedalus_macros::node;

#[node(id = "ui.text", inputs(a), outputs(out))]
pub fn text_error(a: i64) -> Result<i64, String> {
    Ok(a)
}

fn main() {}
