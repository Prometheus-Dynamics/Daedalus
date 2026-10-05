//! The blink firmware in loaded mode: the interpreter starts with plan A (`graph.json`) and,
//! after 4096 ticks, loads plan B (`graph_b.json`, other wiring). A real firmware receives the
//! blob over UART/USB/BLE or reads it from a flash partition (docs/mcu.md); here both are in
//! flash. Measured by `scripts/ci.sh mcu`; an empty `main` on other targets.
#![cfg_attr(target_os = "none", no_std, no_main)]

#[cfg(target_os = "none")]
mod firmware {
    use core::hint::black_box;

    use daedalus_mcu_blink::loaded::{Interpreter, LIBRARY, PLAN_A, PLAN_B};
    use panic_halt as _;

    #[cortex_m_rt::entry]
    fn main() -> ! {
        let interp = cortex_m::singleton!(: Interpreter = Interpreter::new(&LIBRARY)).unwrap();
        let mut ports = None;
        let mut raw: u16 = 0;
        let mut tick: u32 = 0;
        loop {
            tick = tick.wrapping_add(1);
            if tick == 1 || tick == 4096 {
                let blob = if tick == 1 { PLAN_A } else { PLAN_B };
                if interp.load(black_box(blob)).is_err() {
                    cortex_m::asm::bkpt();
                }
                ports = Some((
                    interp.input("sample"),
                    interp.output("led"),
                    interp.output("level"),
                    interp.output("rises"),
                ));
            }
            let Some((Some(sample), Some(led), Some(level), Some(rises))) = ports else {
                cortex_m::asm::bkpt();
                continue;
            };
            raw = black_box((raw + 97) % 4096);
            let _ = interp.push(sample, raw);
            if interp.tick().is_err() {
                cortex_m::asm::bkpt();
            }
            if let Ok(Some(on)) = interp.pop::<bool>(led) {
                black_box(on);
            }
            if let Ok(Some(volts)) = interp.pop::<f32>(level) {
                black_box(volts);
            }
            while let Ok(Some(count)) = interp.pop::<u32>(rises) {
                black_box(count);
            }
        }
    }
}

#[cfg(not(target_os = "none"))]
fn main() {}
