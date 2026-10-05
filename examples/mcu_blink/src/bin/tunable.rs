//! The blink firmware in compiled + tunable mode: as `src/main.rs`, plus a parameter update
//! message (as a UART or BLE receive would deliver it) applied between ticks. Measured by
//! `scripts/ci.sh mcu`; an empty `main` on other targets.
#![cfg_attr(target_os = "none", no_std, no_main)]

#[cfg(target_os = "none")]
mod firmware {
    use core::hint::black_box;

    use daedalus_mcu::Tunable;
    use daedalus_mcu_blink::TunableGraph;
    use panic_halt as _;

    /// `threshold.on = 2.5` (`daedalus-mcu param tunable.json threshold.on 2.5`): id 1, `f32`.
    static UPDATE: [u8; 6] = [0x01, 0x09, 0x00, 0x00, 0x20, 0x40];

    #[cortex_m_rt::entry]
    fn main() -> ! {
        let graph = cortex_m::singleton!(: TunableGraph = TunableGraph::new()).unwrap();
        let mut raw: u16 = 0;
        loop {
            raw = black_box((raw + 97) % 4096);
            if raw < 97 && graph.apply_update(black_box(&UPDATE)).is_err() {
                cortex_m::asm::bkpt();
            }
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
