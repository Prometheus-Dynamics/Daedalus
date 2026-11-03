//! The firmware's loop on the host with a mock board: every mode drives the LED the same way
//! from the same ADC samples, and the tick timer is read once before and once after each step.

use std::cell::Cell;

use daedalus_mcu_blink::loaded::{Interpreter, LIBRARY, PLAN_A};
use daedalus_mcu_blink::{Graph, TunableGraph};
use daedalus_mcu_board::{Board, Loaded, Mode, TickStats, run};

/// A sawtooth on the ADC, an LED that records its changes, and a clock that advances 3 µs per
/// reading (so a tick measures 3 µs).
#[derive(Default)]
struct MockBoard {
    raw: u16,
    led: Vec<bool>,
    clock: Cell<u32>,
}

impl Board for MockBoard {
    fn read_adc(&mut self) -> u16 {
        self.raw = (self.raw + 97) % 4096;
        self.raw
    }

    fn set_led(&mut self, on: bool) {
        self.led.push(on);
    }

    fn now_us(&self) -> u32 {
        let now = self.clock.get();
        self.clock.set(now.wrapping_add(3));
        now
    }
}

const TICKS: u32 = 2000;

fn led_trace<M: Mode>(mode: &mut M) -> (Vec<bool>, TickStats) {
    let mut board = MockBoard::default();
    let stats = run(&mut board, mode, TICKS).expect(M::NAME);
    (board.led, stats)
}

#[test]
fn every_mode_drives_the_led_alike() {
    let (compiled, stats) = led_trace(&mut Graph::new());
    assert!(compiled.contains(&true) && compiled.contains(&false));
    assert_eq!(
        stats,
        TickStats {
            ticks: TICKS,
            min_us: 3,
            max_us: 3,
            total_us: 3 * u64::from(TICKS)
        }
    );
    assert_eq!(stats.mean_us(), 3);
    assert_eq!(led_trace(&mut TunableGraph::new()).0, compiled);
    let mut interp = Box::new(Interpreter::new(&LIBRARY));
    let mut loaded = Loaded::new(&mut interp, PLAN_A).expect("plan A loads");
    assert_eq!(led_trace(&mut loaded).0, compiled);
}

#[test]
fn a_wrapping_clock_still_measures_the_tick() {
    let mut board = MockBoard {
        clock: Cell::new(u32::MAX - 1),
        ..MockBoard::default()
    };
    let stats = run(&mut board, &mut Graph::new(), 1).unwrap();
    assert_eq!((stats.min_us, stats.max_us), (3, 3));
}
