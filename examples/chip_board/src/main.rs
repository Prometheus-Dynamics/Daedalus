//! The blink graph on a Raspberry Pi Pico (`thumbv6m-none-eabi`): ADC0 (GPIO26) in, the LED
//! (GPIO25) out. Runs each mode for `TICKS` ticks and reports its tick times (the RP2040's 1 MHz
//! timer) and stack high-water mark over defmt-RTT (`probe-rs run`), then repeats. Other targets
//! get an empty `main`.
#![cfg_attr(target_os = "none", no_std, no_main)]

#[cfg(pico)]
mod firmware {
    use daedalus_chip_blink::Graph;
    use daedalus_chip_blink::loaded::{Interpreter, LIBRARY, PLAN_A};
    use daedalus_chip_blink::tunable::Graph as TunableGraph;
    use daedalus_chip_board::{Board, Loaded, Mode, run, stack};
    use defmt_rtt as _;
    use embedded_hal::digital::OutputPin;
    use panic_halt as _;
    use rp2040_hal::{self as hal, pac};

    /// The second stage bootloader for the Pico's W25Q080 flash.
    // SAFETY: nothing else is placed in `.boot2`, and the linker script puts it first in flash.
    #[unsafe(link_section = ".boot2")]
    #[used]
    static BOOT2_FIRMWARE: [u8; 256] = rp2040_boot2::BOOT_LOADER_W25Q080;

    const XTAL_HZ: u32 = 12_000_000;
    const TICKS: u32 = 10_000;

    type Led =
        hal::gpio::Pin<hal::gpio::bank0::Gpio25, hal::gpio::FunctionSioOutput, hal::gpio::PullDown>;
    type AdcInput = hal::adc::AdcPin<
        hal::gpio::Pin<hal::gpio::bank0::Gpio26, hal::gpio::FunctionSioInput, hal::gpio::PullNone>,
    >;

    struct Pico {
        adc: hal::Adc,
        input: AdcInput,
        led: Led,
        timer: hal::Timer,
    }

    impl Board for Pico {
        fn read_adc(&mut self) -> u16 {
            let sample: u16 = self.adc.read(&mut self.input).unwrap_or(0);
            sample
        }

        fn set_led(&mut self, on: bool) {
            let _ = self.led.set_state(on.into());
        }

        fn now_us(&self) -> u32 {
            self.timer.get_counter_low()
        }
    }

    fn report<M: Mode>(board: &mut Pico, mode: &mut M) {
        stack::paint();
        match run(board, mode, TICKS) {
            Ok(stats) => defmt::info!(
                "{=str}: {=u32} ticks, {=u32}..{=u32} us, mean {=u32} us, stack {=usize} B",
                M::NAME,
                stats.ticks,
                stats.min_us,
                stats.max_us,
                stats.mean_us(),
                stack::used()
            ),
            Err(_) => defmt::error!("{=str}: a tick failed", M::NAME),
        }
    }

    #[hal::entry]
    fn main() -> ! {
        let mut pac = pac::Peripherals::take().unwrap();
        let mut watchdog = hal::Watchdog::new(pac.WATCHDOG);
        let clocks = hal::clocks::init_clocks_and_plls(
            XTAL_HZ,
            pac.XOSC,
            pac.CLOCKS,
            pac.PLL_SYS,
            pac.PLL_USB,
            &mut pac.RESETS,
            &mut watchdog,
        )
        .ok()
        .unwrap();
        let sio = hal::Sio::new(pac.SIO);
        let pins = hal::gpio::Pins::new(
            pac.IO_BANK0,
            pac.PADS_BANK0,
            sio.gpio_bank0,
            &mut pac.RESETS,
        );
        let mut board = Pico {
            adc: hal::Adc::new(pac.ADC, &mut pac.RESETS),
            input: hal::adc::AdcPin::new(pins.gpio26.into_floating_input()).unwrap(),
            led: pins.gpio25.into_push_pull_output(),
            timer: hal::Timer::new(pac.TIMER, &mut pac.RESETS, &clocks),
        };

        // The graphs live in statics, so the stack measured is the tick's own.
        let compiled = cortex_m::singleton!(: Graph = Graph::new()).unwrap();
        let tunable = cortex_m::singleton!(: TunableGraph = TunableGraph::new()).unwrap();
        let runner = cortex_m::singleton!(: Interpreter = Interpreter::new()).unwrap();
        let Ok(mut loaded) = Loaded::new(runner, &LIBRARY, PLAN_A) else {
            defmt::error!("plan A does not load");
            loop {
                cortex_m::asm::wfi();
            }
        };
        loop {
            report(&mut board, compiled);
            report(&mut board, tunable);
            report(&mut board, &mut loaded);
        }
    }
}

#[cfg(not(target_os = "none"))]
fn main() {}
