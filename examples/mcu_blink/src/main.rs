//! Firmware running the blink graph on Cortex-M (`thumbv7em-none-eabihf`, `thumbv6m-none-eabi`),
//! built and measured by `scripts/ci.sh mcu`. A sawtooth stands in for the ADC and
//! `black_box` for the GPIO, so the measured size is the graph and its runtime, without a HAL.
//! Other targets get an empty `main`, so workspace builds keep working.
#![cfg_attr(target_os = "none", no_std, no_main)]

#[cfg(target_os = "none")]
mod firmware {
    use core::hint::black_box;

    use daedalus_mcu_blink::Graph;
    use panic_halt as _;

    #[cortex_m_rt::entry]
    fn main() -> ! {
        // The whole graph (queues and node state) in one static, initialised once.
        let graph = cortex_m::singleton!(: Graph = Graph::new()).unwrap();
        let mut raw: u16 = 0;
        loop {
            raw = black_box((raw + 97) % 4096);
            // `sample` is latest-only: a push never fails.
            let _ = graph.push_sample(raw);
            if graph.tick().is_err() {
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
