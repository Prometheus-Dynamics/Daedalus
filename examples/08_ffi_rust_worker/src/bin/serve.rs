//! The Rust worker the host starts: it serves the example's nodes on stdin and stdout.

fn main() {
    if let Err(error) = daedalus_lang_rust::serve(daedalus_example_rust_worker::NODES) {
        let _ = std::io::Write::write_fmt(&mut std::io::stderr(), format_args!("{error}\n"));
        std::process::exit(1);
    }
}
