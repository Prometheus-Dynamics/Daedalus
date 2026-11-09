//! The blink graph (`examples/chip_blink`) on a board: an ADC sample in, the LED out, in each of
//! the three modes (compiled, tunable, loaded), timing every tick.
//!
//! [`Board`] is what the firmware needs from a HAL. `src/main.rs` implements it for the Raspberry
//! Pi Pico with `rp2040-hal` and reports [`TickStats`] and the stack high-water mark over
//! defmt-RTT. The loop is the same code on the host, where `tests/host.rs` drives it with a mock
//! board.

#![no_std]

use daedalus_cpu::blob::Library;
use daedalus_cpu::blob::{Interpreter, LoadError, RunError};
use daedalus_cpu::chip::ChipError;

#[cfg(all(target_arch = "arm", target_os = "none"))]
pub mod stack;

/// What the firmware needs from a board's HAL.
pub trait Board {
    /// One 12-bit sample of the analogue input.
    fn read_adc(&mut self) -> u16;
    fn set_led(&mut self, on: bool);
    /// A free-running microsecond counter. It may wrap.
    fn now_us(&self) -> u32;
}

/// A tick that failed, in whichever mode ran it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TickError {
    Compiled(ChipError),
    Loaded(RunError),
}

/// The blink graph in one mode, as the firmware drives it.
pub trait Mode {
    const NAME: &'static str;

    /// Pushes `sample`, ticks, drains every output, and returns the LED state if the tick set one.
    fn step(&mut self, sample: u16) -> Result<Option<bool>, TickError>;
}

macro_rules! compiled_mode {
    ($graph:ty, $name:literal) => {
        impl Mode for $graph {
            const NAME: &'static str = $name;

            fn step(&mut self, sample: u16) -> Result<Option<bool>, TickError> {
                // `sample` keeps only the latest value, so a push can't fail.
                let _ = self.push_sample(sample);
                self.tick().map_err(TickError::Compiled)?;
                let _ = self.pop_level();
                while self.pop_rises().is_some() {}
                Ok(self.pop_led())
            }
        }
    };
}

compiled_mode!(daedalus_chip_blink::graph::Graph, "compiled");
compiled_mode!(daedalus_chip_blink::tunable::Graph, "tunable");

/// Why a plan didn't start.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartError {
    Load(LoadError),
    MissingPort,
}

/// The small runner with a plan loaded and its ports looked up.
pub struct Loaded<'a, const ARENA: usize> {
    runner: &'a mut Interpreter<ARENA>,
    ports: [u16; 4],
}

impl<'a, const ARENA: usize> Loaded<'a, ARENA> {
    /// Loads `blob` into `runner`.
    pub fn new(
        runner: &'a mut Interpreter<ARENA>,
        library: &Library,
        blob: &[u8],
    ) -> Result<Self, StartError> {
        runner.load(library, blob).map_err(StartError::Load)?;
        let port = |port: Option<u16>| port.ok_or(StartError::MissingPort);
        let ports = [
            port(runner.input("sample"))?,
            port(runner.output("led"))?,
            port(runner.output("level"))?,
            port(runner.output("rises"))?,
        ];
        Ok(Self { runner, ports })
    }
}

impl<const ARENA: usize> Mode for Loaded<'_, ARENA> {
    const NAME: &'static str = "loaded";

    fn step(&mut self, sample: u16) -> Result<Option<bool>, TickError> {
        let [input, led, level, rises] = self.ports;
        let _ = self.runner.push(input, sample);
        self.runner.tick().map_err(TickError::Loaded)?;
        self.runner.pop::<f32>(level).map_err(TickError::Loaded)?;
        while self
            .runner
            .pop::<u32>(rises)
            .map_err(TickError::Loaded)?
            .is_some()
        {}
        self.runner.pop::<bool>(led).map_err(TickError::Loaded)
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
        self.total_us
            .checked_div(u64::from(self.ticks))
            .map_or(0, |mean| u32::try_from(mean).unwrap_or(u32::MAX))
    }
}

/// Runs `ticks` ticks of `mode` on `board`: reads the ADC, times `step`, and drives the LED with
/// what the graph decided.
pub fn run<B: Board, M: Mode>(
    board: &mut B,
    mode: &mut M,
    ticks: u32,
) -> Result<TickStats, TickError> {
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
