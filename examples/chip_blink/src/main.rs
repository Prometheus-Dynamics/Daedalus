//! The blink graph on a Cortex-M. A sawtooth stands in for the ADC and `black_box` for the GPIO,
//! so what `scripts/ci.d/chip.sh` measures is the graph and its runtime. Other targets get an
//! empty `main`, so workspace builds keep working.

#![cfg_attr(target_os = "none", no_std, no_main)]
#![forbid(unsafe_code)]

#[cfg(target_os = "none")]
mod firmware {
    use core::hint::black_box;

    use daedalus_chip_blink::Graph;
    use panic_halt as _;

    #[cortex_m_rt::entry]
    fn main() -> ! {
        // `entry` turns this into a `&'static mut` that only `main` can reach.
        static mut GRAPH: Graph = Graph::new();
        let graph: &'static mut Graph = GRAPH;
        let (mut raw, mut now): (u16, u64) = (0, 0);
        loop {
            raw = black_box((raw + 97) % 4096);
            // `sample` keeps the latest, so a push never fails.
            let _ = graph.push_sample(raw);
            now += 1;
            if graph.tick_at(now).is_err() {
                cortex_m::asm::bkpt();
            }
            if let Some(led) = graph.pop_led() {
                black_box(led);
            }
            if let Some(level) = graph.pop_level() {
                black_box(level);
            }
            while let Some(rises) = graph.pop_rises() {
                black_box(rises);
            }
        }
    }
}

#[cfg(not(target_os = "none"))]
fn main() {}
