use daedalus::macros::node;

#[node(id = "ui.unknown", bundle = "starter")]
pub fn unknown_argument() -> Result<(), daedalus::runtime::NodeError> {
    Ok(())
}

fn main() {}
