use daedalus_derive::NodeConfig;

fn check(_: &Settings) -> Result<(), &'static str> {
    Ok(())
}

#[derive(Clone, Default, NodeConfig)]
#[validate(fn = check)]
pub struct Settings {
    #[port(default = 3, min = 1, max = 9)]
    pub size: u32,
    pub flag: bool,
}

fn main() {}
