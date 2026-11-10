//! The Rust fixture worker: serves the fixture nodes this SDK can declare, on stdin and stdout.

#![forbid(unsafe_code)]

mod nodes;

fn main() {
    if let Err(error) = daedalus_lang_rust::serve(nodes::NODES) {
        let _ = std::io::Write::write_fmt(&mut std::io::stderr(), format_args!("{error}\n"));
        std::process::exit(1);
    }
}
