//! The blink firmware with stored plans: the small runner starts with plan A and later loads
//! plan B, with other wiring. A real firmware receives a plan over UART, USB or BLE. An empty
//! `main` on other targets.

#![cfg_attr(target_os = "none", no_std, no_main)]
#![forbid(unsafe_code)]

#[cfg(target_os = "none")]
mod firmware {
    use core::hint::black_box;

    use daedalus_chip_blink::loaded::{Interpreter, LIBRARY, PLAN_A, PLAN_B};
    use panic_halt as _;

    #[cortex_m_rt::entry]
    fn main() -> ! {
        static mut RUNNER: Interpreter = Interpreter::new();
        let runner: &'static mut Interpreter = RUNNER;
        let mut ports = None;
        let (mut raw, mut now): (u16, u64) = (0, 0);
        loop {
            now += 1;
            if now == 1 || now == 4096 {
                let plan = if now == 1 { PLAN_A } else { PLAN_B };
                if runner.load(&LIBRARY, black_box(plan)).is_err() {
                    cortex_m::asm::bkpt();
                }
                ports = Some((
                    runner.input("sample"),
                    runner.output("led"),
                    runner.output("level"),
                    runner.output("rises"),
                ));
            }
            let Some((Some(sample), Some(led), Some(level), Some(rises))) = ports else {
                cortex_m::asm::bkpt();
                continue;
            };
            raw = black_box((raw + 97) % 4096);
            let _ = runner.push(sample, raw);
            if runner.tick_at(now).is_err() {
                cortex_m::asm::bkpt();
            }
            if let Ok(Some(on)) = runner.pop::<bool>(led) {
                black_box(on);
            }
            if let Ok(Some(volts)) = runner.pop::<f32>(level) {
                black_box(volts);
            }
            while let Ok(Some(count)) = runner.pop::<u32>(rises) {
                black_box(count);
            }
        }
    }
}

#[cfg(not(target_os = "none"))]
fn main() {}
