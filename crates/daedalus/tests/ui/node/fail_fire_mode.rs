use daedalus::macros::node;

#[node(id = "ui.fire", inputs("a"), outputs("out"), fire = "every")]
pub fn fire_mode(a: i64) -> Result<i64, daedalus::runtime::NodeError> {
    Ok(a)
}

fn main() {}
