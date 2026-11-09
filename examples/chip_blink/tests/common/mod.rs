//! Shared by the native tests: an allocation counter and the blink graph computed by calling the
//! node functions by hand.
#![allow(dead_code)]

use daedalus_abi::StepCx;
use daedalus_abi::counting::CountingAlloc;
use daedalus_chip_blink::loaded::{FANIN_ARENA, LIBRARY};
use daedalus_chip_blink_nodes::{Edges, Lowpass, blink, lowpass, rising, scale, threshold};
use daedalus_cpu::blob::Interpreter;

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

/// What one tick puts on `level`, `led` and `rises`.
pub type Out = (Option<f32>, Option<bool>, Option<u32>);

/// Low, a step above the `on` threshold, down below `off`, and up again.
pub fn signal() -> impl Iterator<Item = u16> {
    (0..200_u32).map(|t| match t % 100 {
        0..20 => 100,
        20..60 => 3500,
        _ => 400,
    })
}

/// The tunable constants of the blink graph.
#[derive(Clone, Copy)]
pub struct Params {
    pub alpha: f32,
    pub on: f32,
    pub off: f32,
    pub period: u32,
}

impl Params {
    /// The constants of `graph.json`.
    pub const A: Params = Params {
        alpha: 0.25,
        on: 2.0,
        off: 1.0,
        period: 4,
    };
}

/// One tick of the graph, from the node functions called by hand.
pub struct Reference {
    now: u64,
    lowpass: Lowpass,
    active: bool,
    edges: Edges,
    pub params: Params,
    /// `graph.json`, or else `graph_b.json`: unfiltered, period 2.
    filtered: bool,
}

impl Reference {
    pub fn a() -> Reference {
        Reference::new(Params::A, true)
    }

    pub fn b() -> Reference {
        let params = Params {
            period: 2,
            ..Params::A
        };
        Reference::new(params, false)
    }

    /// `graph_fanin_b.json`: filtered, period 2.
    pub fn fanin_b() -> Reference {
        let params = Params {
            period: 2,
            ..Params::A
        };
        Reference::new(params, true)
    }

    /// `graph.json` with `alpha` 0.5, `on` 2.5 and `period` 2.
    pub fn tuned() -> Reference {
        let params = Params {
            alpha: 0.5,
            on: 2.5,
            period: 2,
            ..Params::A
        };
        Reference::new(params, true)
    }

    fn new(params: Params, filtered: bool) -> Reference {
        Reference {
            now: 0,
            lowpass: Lowpass::default(),
            active: false,
            edges: Edges::default(),
            params,
            filtered,
        }
    }

    pub fn step(&mut self, raw: u16) -> Out {
        let params = self.params;
        let mut cx = StepCx::new();
        cx.now_ns = self.now;
        self.now += 1;
        let volts = scale(raw, 0.0008);
        let level = if self.filtered {
            lowpass(volts, params.alpha, &mut self.lowpass)
        } else {
            volts
        };
        let active = threshold(&level, params.on, params.off, &mut self.active);
        let led = blink(&cx, active, params.period);
        (Some(level), Some(led), rising(active, &mut self.edges))
    }
}

/// A stored plan running, by port name.
pub struct Loaded {
    pub running: Box<Interpreter<FANIN_ARENA>>,
    pub ports: [u16; 4],
    now: u64,
}

impl Loaded {
    pub fn new(plan: &[u8]) -> Loaded {
        let mut loaded = Loaded {
            running: Box::new(Interpreter::new()),
            ports: [0; 4],
            now: 0,
        };
        loaded.load(plan);
        loaded
    }

    /// Loads `plan`, and starts its clock again.
    pub fn load(&mut self, plan: &[u8]) {
        self.running.load(&LIBRARY, plan).unwrap();
        self.ports = [
            self.running.input("sample").unwrap(),
            self.running.output("level").unwrap(),
            self.running.output("led").unwrap(),
            self.running.output("rises").unwrap(),
        ];
        self.now = 0;
    }

    pub fn step(&mut self, raw: u16) -> Out {
        let [sample, level, led, rises] = self.ports;
        self.running.push(sample, raw).unwrap();
        self.running.tick_at(self.now).unwrap();
        self.now += 1;
        (
            self.running.pop(level).unwrap(),
            self.running.pop(led).unwrap(),
            self.running.pop(rises).unwrap(),
        )
    }
}

/// `body` with its checksum, as a stored plan ends.
pub fn sealed(body: &[u8]) -> Vec<u8> {
    [body, &daedalus_plan::crc32(body).to_le_bytes()].concat()
}
