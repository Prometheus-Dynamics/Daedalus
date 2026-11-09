//! The blink firmware with parameters: the graph runs as generated code, and a message changes
//! a constant while it does. An empty `main` on other targets.

#![cfg_attr(target_os = "none", no_std, no_main)]
#![forbid(unsafe_code)]

#[cfg(target_os = "none")]
mod firmware {
    use core::hint::black_box;

    use daedalus_chip_blink::tunable::Graph;
    use daedalus_cpu::chip::Tunable;
    use panic_halt as _;

    #[cortex_m_rt::entry]
    fn main() -> ! {
        static mut GRAPH: Graph = Graph::new();
        let graph: &'static mut Graph = GRAPH;
        let (mut raw, mut now): (u16, u64) = (0, 0);
        loop {
            raw = black_box((raw + 97) % 4096);
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
            // A real firmware reads this from UART, USB or BLE.
            let message = black_box([0x01_u8, 0x09, 0x00, 0x00, 0x20, 0x40]);
            let _ = black_box(graph.apply_update(&message));
        }
    }
}

#[cfg(not(target_os = "none"))]
fn main() {}
