//! `wasm32-wasip1` smoke command (`scripts/ci.sh wasm`, run by `scripts/wasi-smoke.mjs`): WASI
//! has an OS clock (`platform::OS_CLOCK`) but no threads, so the engine times the graph with the
//! platform clock while `Parallel` and `Adaptive` run serially. Exits with [`smoke_with`]'s
//! status.

use daedalus::engine::Clock;
use daedalus_wasm_smoke::smoke_with;

fn main() {
    std::process::exit(smoke_with(Clock::default()));
}
