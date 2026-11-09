//! The worker binary: serves `daedalus_plugins_rust_worker::worker()` over stdin and stdout.

fn main() -> std::io::Result<()> {
    daedalus_plugins_rust_worker::worker()
        .map_err(std::io::Error::other)?
        .serve()
}
