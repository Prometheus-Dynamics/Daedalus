use daedalus::plugin;

#[plugin(id = "ui.foreign", foreign_types(std::time::Duration))]
struct UiForeignPlugin;

fn main() {}
