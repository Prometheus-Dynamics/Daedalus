//! The blink graph (`examples/mcu_blink`) on a board: an ADC sample in, the LED out, in each of
//! the three modes (compiled, compiled + tunable, loaded), timing every tick.
//!
//! [`Board`] is what the firmware needs from a HAL; `src/main.rs` implements it for the
//! Raspberry Pi Pico with `rp2040-hal` (ADC0 on GPIO26, the LED on GPIO25, the 1 MHz timer) and
//! reports [`TickStats`] and the stack high-water mark over defmt-RTT. The loop is the same code
//! on the host, where `tests/host.rs` drives it with a mock board. Written, untested on a board.
#![no_std]

use daedalus_mcu::McuError;
use daedalus_mcu_blink::loaded::Interpreter;
use daedalus_mcu_blink::{Graph, TunableGraph};

#[cfg(all(target_arch = "arm", target_os = "none"))]
pub mod stack;

/// What the firmware needs from a board's HAL.
pub trait Board {
    /// One 12-bit sample of the analogue input.
    fn read_adc(&mut self) -> u16;
    fn set_led(&mut self, on: bool);
    /// A free-running microsecond counter; it may wrap.
    fn now_us(&self) -> u32;
}

/// The blink graph in one mode, as the firmware drives it.
pub trait Mode {
    const NAME: &'static str;

    /// Push `sample`, tick, drain every output, and return the LED state if the tick set one.
    fn step(&mut self, sample: u16) -> Result<Option<bool>, McuError>;
}

impl Mode for Graph {
    const NAME: &'static str = "compiled";

    fn step(&mut self, sample: u16) -> Result<Option<bool>, McuError> {
        // `sample` is latest-only: a push never fails.
        let _ = self.push_sample(sample);
        self.tick()?;
        let _ = self.pop_level();
        while self.pop_rises().is_some() {}
        Ok(self.pop_led())
    }
}

impl Mode for TunableGraph {
    const NAME: &'static str = "tunable";

    fn step(&mut self, sample: u16) -> Result<Option<bool>, McuError> {
        let _ = self.push_sample(sample);
        self.tick()?;
        let _ = self.pop_level();
        while self.pop_rises().is_some() {}
        Ok(self.pop_led())
    }
}

/// The interpreter with a plan loaded and its ports looked up.
pub struct Loaded<'a> {
    interp: &'a mut Interpreter,
    ports: [u16; 4],
}

impl<'a> Loaded<'a> {
    /// Load `blob` into `interp` (between ticks, as on the device).
    pub fn new(interp: &'a mut Interpreter, blob: &[u8]) -> Option<Self> {
        interp.load(blob).ok()?;
        let ports = [
            interp.input("sample")?,
            interp.output("led")?,
            interp.output("level")?,
            interp.output("rises")?,
        ];
        Some(Self { interp, ports })
    }
}

impl Mode for Loaded<'_> {
    const NAME: &'static str = "loaded";

    fn step(&mut self, sample: u16) -> Result<Option<bool>, McuError> {
        let [input, led, level, rises] = self.ports;
        let _ = self.interp.push(input, sample);
        self.interp.tick()?;
        self.interp.pop::<f32>(level)?;
        while self.interp.pop::<u32>(rises)?.is_some() {}
        self.interp.pop::<bool>(led)
    }
}

/// Tick times over a run, in microseconds of the board's counter.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TickStats {
    pub ticks: u32,
    pub min_us: u32,
    pub max_us: u32,
    pub total_us: u64,
}

impl TickStats {
    fn record(&mut self, us: u32) {
        self.min_us = if self.ticks == 0 {
            us
        } else {
            self.min_us.min(us)
        };
        self.max_us = self.max_us.max(us);
        self.total_us += u64::from(us);
        self.ticks += 1;
    }

    pub fn mean_us(&self) -> u32 {
        if self.ticks == 0 {
            0
        } else {
            (self.total_us / u64::from(self.ticks)) as u32
        }
    }
}

/// Run `ticks` ticks of `mode` on `board`: read the ADC, time `step` (push, tick, drain), and
/// drive the LED with what the graph decided.
pub fn run<B: Board, M: Mode>(
    board: &mut B,
    mode: &mut M,
    ticks: u32,
) -> Result<TickStats, McuError> {
    let mut stats = TickStats::default();
    for _ in 0..ticks {
        let sample = board.read_adc();
        let start = board.now_us();
        let led = mode.step(sample)?;
        stats.record(board.now_us().wrapping_sub(start));
        if let Some(on) = led {
            board.set_led(on);
        }
    }
    Ok(stats)
}
