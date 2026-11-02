//! The fan-in graph (`graph_fanin.json`) in all three modes, natively. `blink.active` has two
//! edges: a host command through the `command_active` user adapter, and the threshold. The first
//! edge in plan order that holds a value supplies it, so a pushed command wins over the threshold
//! in plan A; plan B puts the threshold first (a wiring change the device loads, no firmware
//! update), so the command only supplies the ticks on which the threshold has no value. Every mode
//! matches the node functions called by hand, and pushes, ticks and pops never allocate.

mod common;

use common::{allocations, signal};
use daedalus_mcu::loaded::LoadError;
use daedalus_mcu::{Ctx, NodeState, Tunable};
use daedalus_mcu_blink::fanin::FaninGraph;
use daedalus_mcu_blink::fanin_loaded::{ARENA, Interpreter, LIBRARY, PLAN_A, PLAN_B};
use daedalus_mcu_blink::fanin_tunable::FaninTunableGraph;
use daedalus_mcu_blink::nodes;

const FAN_IN_MANIFEST: &str = include_str!("../generated/fanin_plan_a.json");

/// One tick's pushes: a sample on most ticks, a command on some.
fn stream() -> impl Iterator<Item = (Option<u16>, Option<u8>)> {
    signal().enumerate().map(|(t, raw)| {
        let sample = (t % 9 != 8).then_some(raw);
        let command = match t % 13 {
            4 => Some(0),
            5 => Some(1),
            _ => None,
        };
        (sample, command)
    })
}

/// The fan-in graph computed by hand: the node functions in schedule order, with `blink.active`
/// fed by the command (through `command_active`) and by the threshold.
struct Reference {
    tick: u32,
    lowpass: nodes::Lowpass,
    threshold: bool,
    edges: nodes::Edges,
    alpha: f32,
    on: f32,
    off: f32,
    period: u32,
    /// Plan A: the command edge comes first; plan B: the threshold does.
    command_first: bool,
}

impl Reference {
    fn new(command_first: bool) -> Self {
        Self {
            tick: 0,
            lowpass: <nodes::Lowpass as NodeState>::INIT,
            threshold: false,
            edges: <nodes::Edges as NodeState>::INIT,
            alpha: 0.25,
            on: 2.0,
            off: 1.0,
            period: 4,
            command_first,
        }
    }

    /// One tick: `(led, rises)`, each present when its node ran and produced.
    fn step(&mut self, sample: Option<u16>, command: Option<u8>) -> (Option<bool>, Option<u32>) {
        let ctx = Ctx {
            tick: self.tick,
            now_micros: 0,
        };
        self.tick = self.tick.wrapping_add(1);
        // `threshold` (and `rising`) run when a sample reached them through scale and lowpass.
        let threshold = sample.map(|raw| {
            let volts = nodes::scale(f32::from(raw), 0.0008);
            let level = nodes::lowpass(volts, self.alpha, &mut self.lowpass);
            nodes::threshold(&level, self.on, self.off, &mut self.threshold)
        });
        let rises = threshold.and_then(|active| nodes::rising(active, &mut self.edges));
        let command = command.map(nodes::command_active);
        let active = if self.command_first {
            command.or(threshold)
        } else {
            threshold.or(command)
        };
        let led = active.map(|active| nodes::blink(&ctx, active, self.period));
        (led, rises)
    }
}

/// The fan-in graph in compiled mode, with its host ports by name.
fn compiled(
    graph: &mut FaninGraph,
    sample: Option<u16>,
    command: Option<u8>,
) -> (Option<bool>, Option<u32>) {
    if let Some(command) = command {
        graph.push_command(command).unwrap();
    }
    if let Some(raw) = sample {
        graph.push_sample(raw).unwrap();
    }
    graph.tick().unwrap();
    (graph.pop_led(), graph.pop_rises())
}

/// The fan-in graph in loaded mode: the interpreter and the ports of the loaded plan.
struct Loaded {
    interp: Box<Interpreter>,
    /// `sample`, `command`, `led`, `rises`.
    ports: [u16; 4],
}

impl Loaded {
    fn new(blob: &[u8]) -> Self {
        let mut loaded = Self {
            interp: Box::new(Interpreter::new(&LIBRARY)),
            ports: [0; 4],
        };
        loaded.load(blob);
        loaded
    }

    fn load(&mut self, blob: &[u8]) {
        self.interp.load(blob).unwrap();
        let interp = &self.interp;
        self.ports = [
            interp.input("sample").unwrap(),
            interp.input("command").unwrap(),
            interp.output("led").unwrap(),
            interp.output("rises").unwrap(),
        ];
    }

    fn step(&mut self, sample: Option<u16>, command: Option<u8>) -> (Option<bool>, Option<u32>) {
        let [sample_port, command_port, led, rises] = self.ports;
        if let Some(command) = command {
            self.interp.push(command_port, command).unwrap();
        }
        if let Some(raw) = sample {
            self.interp.push(sample_port, raw).unwrap();
        }
        self.interp.tick().unwrap();
        (
            self.interp.pop(led).unwrap(),
            self.interp.pop(rises).unwrap(),
        )
    }
}

#[test]
fn plans_fit_the_firmware_arena_and_name_the_fan_in() {
    assert!(Interpreter::required(&LIBRARY, PLAN_A).unwrap() <= ARENA);
    assert!(Interpreter::required(&LIBRARY, PLAN_B).unwrap() <= ARENA);
    assert!(FAN_IN_MANIFEST.contains("\"host.command -> blink.active\""));
    assert!(FAN_IN_MANIFEST.contains("\"threshold.active -> blink.active\""));
}

#[test]
fn compiled_fan_in_matches_the_node_functions_without_allocating() {
    let mut graph = FaninGraph::new();
    let mut reference = Reference::new(true);
    let (mut led_on, mut led_off, mut rises) = (0, 0, 0);
    let before = allocations();
    for (sample, command) in stream() {
        let got = compiled(&mut graph, sample, command);
        assert_eq!(got, reference.step(sample, command));
        match got.0 {
            Some(true) => led_on += 1,
            Some(false) => led_off += 1,
            None => {}
        }
        rises += got.1.is_some() as u32;
    }
    assert_eq!(allocations(), before, "a tick or a push allocated");
    // The stream reaches every case: the blink on and off, and a rising edge.
    assert!(led_on > 0 && led_off > 0 && rises > 0);
}

#[test]
fn a_pushed_command_takes_precedence_over_the_threshold() {
    let mut graph = FaninGraph::new();
    // A high sample keeps the threshold on, so `blink.active` would be on without a command.
    for _ in 0..8 {
        compiled(&mut graph, Some(4000), None);
    }
    // A zero command switches the blink off, whatever the threshold says.
    assert_eq!(compiled(&mut graph, Some(4000), Some(0)).0, Some(false));
    // Without a command the threshold drives it again.
    assert!(compiled(&mut graph, Some(4000), None).0.is_some());
}

#[test]
fn tunable_fan_in_matches_compiled_and_follows_its_parameters() {
    let mut graph = FaninTunableGraph::new();
    let mut reference = Reference::new(true);
    let before = allocations();
    for (t, (sample, command)) in stream().enumerate() {
        if t == 60 {
            // Parameters change between ticks, as on the device.
            graph.set_blink_period(2).unwrap();
            reference.period = 2;
            graph.set_lowpass_alpha(0.5).unwrap();
            reference.alpha = 0.5;
        }
        if let Some(command) = command {
            graph.push_command(command).unwrap();
        }
        if let Some(raw) = sample {
            graph.push_sample(raw).unwrap();
        }
        graph.tick().unwrap();
        let got = (graph.pop_led(), graph.pop_rises());
        assert_eq!(got, reference.step(sample, command), "tick {t}");
    }
    assert_eq!(allocations(), before);
    assert_eq!(graph.param(0), Some(daedalus_mcu::Scalar::F32(0.5)));
}

#[test]
fn loaded_fan_in_plan_matches_compiled_and_plan_b_ignores_the_command() {
    let mut loaded = Loaded::new(PLAN_A);
    let mut reference = Reference::new(true);
    let before = allocations();
    for (sample, command) in stream() {
        assert_eq!(
            loaded.step(sample, command),
            reference.step(sample, command)
        );
    }
    assert_eq!(allocations(), before, "a tick or a push allocated");

    // Plan B (the threshold is the first edge): the threshold decides on every tick it has a
    // value, and the command supplies only the ticks it is silent (no sample).
    let mut reference = Reference::new(false);
    loaded.load(PLAN_B);
    let mut with_commands = Vec::new();
    for (sample, command) in stream() {
        let got = loaded.step(sample, command);
        assert_eq!(got, reference.step(sample, command));
        with_commands.push(got);
    }
    loaded.load(PLAN_B);
    let without: Vec<_> = stream()
        .map(|(sample, _)| loaded.step(sample, None))
        .collect();
    let mut differs = false;
    for (((sample, _), with), without) in stream().zip(&with_commands).zip(&without) {
        match sample {
            Some(_) => assert_eq!(with, without, "the threshold wins where it has a value"),
            None => differs |= with != without,
        }
    }
    assert!(differs, "the command never supplied a silent tick");
}

#[test]
fn rejects_the_blob_format_of_another_version() {
    let mut interp = Interpreter::new(&LIBRARY);
    let mut old = PLAN_A.to_vec();
    old[4] = 2; // the format version byte of blob format 2
    assert_eq!(interp.load(&old), Err(LoadError::Version { found: 2 }));
    interp.load(PLAN_A).unwrap();
}
