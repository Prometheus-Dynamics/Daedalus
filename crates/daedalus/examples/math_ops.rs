//! Demonstrates the math plugin's capability-driven numeric nodes across integer and float flows.
//! Run with:
//!   cargo run -p daedalus-rs --features "engine,plugins" --example math_ops

#![cfg(all(feature = "engine", feature = "plugins"))]

use daedalus::{
    declare_plugin,
    engine::{Engine, EngineConfig, GpuBackend, RuntimeMode},
    graph_builder::GraphBuilder,
    macros::node,
    runtime::{
        BackpressureStrategy, EdgePolicyKind, NodeError,
        plugins::{PluginRegistry, RegistryPluginExt},
    },
};
use std::ops::Add;

trait PowOps: Copy {
    fn pow(self, exp: Self) -> Self;
}

impl PowOps for i32 {
    fn pow(self, exp: Self) -> Self {
        let exp_u32 = exp.max(0) as u32;
        self.pow(exp_u32)
    }
}

impl PowOps for f64 {
    fn pow(self, exp: Self) -> Self {
        self.powf(exp)
    }
}

#[node(id = "add", capability = "Add", inputs("a", "b"), outputs("out"))]
fn add<T: Add<Output = T> + Copy + Send + Sync + 'static>(a: T, b: T) -> Result<T, NodeError> {
    Ok(a + b)
}

#[node(id = "sub", capability = "Sub", inputs("a", "b"), outputs("out"))]
fn sub<T: std::ops::Sub<Output = T> + Copy + Send + Sync + 'static>(
    a: T,
    b: T,
) -> Result<T, NodeError> {
    Ok(a - b)
}

#[node(id = "mul", capability = "Mul", inputs("a", "b"), outputs("out"))]
fn mul<T: std::ops::Mul<Output = T> + Copy + Send + Sync + 'static>(
    a: T,
    b: T,
) -> Result<T, NodeError> {
    Ok(a * b)
}

#[node(id = "div", capability = "Div", inputs("a", "b"), outputs("out"))]
fn div<T: std::ops::Div<Output = T> + Copy + Send + Sync + 'static>(
    a: T,
    b: T,
) -> Result<T, NodeError> {
    Ok(a / b)
}

#[node(id = "rem", capability = "Rem", inputs("a", "b"), outputs("out"))]
fn rem<T: std::ops::Rem<Output = T> + Copy + Send + Sync + 'static>(
    a: T,
    b: T,
) -> Result<T, NodeError> {
    Ok(a % b)
}

#[node(id = "min", capability = "Min", inputs("a", "b"), outputs("out"))]
fn min<T: PartialOrd + Copy + Send + Sync + 'static>(a: T, b: T) -> Result<T, NodeError> {
    Ok(if a <= b { a } else { b })
}

#[node(id = "max", capability = "Max", inputs("a", "b"), outputs("out"))]
fn max<T: PartialOrd + Copy + Send + Sync + 'static>(a: T, b: T) -> Result<T, NodeError> {
    Ok(if a >= b { a } else { b })
}

#[node(
    id = "clamp",
    capability = "Clamp",
    inputs("x", "lo", "hi"),
    outputs("out")
)]
fn clamp<T: PartialOrd + Copy + Send + Sync + 'static>(x: T, lo: T, hi: T) -> Result<T, NodeError> {
    Ok(if x < lo {
        lo
    } else if x > hi {
        hi
    } else {
        x
    })
}

#[node(id = "pow", capability = "Pow", inputs("base", "exp"), outputs("out"))]
fn pow<T: PowOps + Send + Sync + 'static>(base: T, exp: T) -> Result<T, NodeError> {
    Ok(base.pow(exp))
}

#[node(id = "src_i32", outputs("a", "b"))]
fn src_i32() -> Result<(i32, i32), NodeError> {
    Ok((12, 5))
}

#[node(id = "src_f64", outputs("a", "b"))]
fn src_f64() -> Result<(f64, f64), NodeError> {
    Ok((7.5, 2.5))
}

#[node(
    id = "sink_i32",
    inputs("sum", "diff", "product", "quot", "rem", "min", "max", "clamp", "pow")
)]
#[allow(clippy::too_many_arguments)]
fn sink_i32(
    sum: i32,
    diff: i32,
    product: i32,
    quot: i32,
    rem_v: i32,
    min_v: i32,
    max_v: i32,
    clamp_v: i32,
    pow_v: i32,
) -> Result<(), NodeError> {
    println!(
        "i32 -> sum={sum}, diff={diff}, product={product}, quot={quot}, rem={rem_v}, min={min_v}, max={max_v}, clamp={clamp_v}, pow={pow_v}"
    );
    Ok(())
}

#[node(
    id = "sink_f64",
    inputs("sum", "diff", "product", "quot", "rem", "min", "max", "clamp", "pow")
)]
#[allow(clippy::too_many_arguments)]
fn sink_f64(
    sum: f64,
    diff: f64,
    product: f64,
    quot: f64,
    rem_v: f64,
    min_v: f64,
    max_v: f64,
    clamp_v: f64,
    pow_v: f64,
) -> Result<(), NodeError> {
    println!(
        "f64 -> sum={sum}, diff={diff}, product={product}, quot={quot}, rem={rem_v}, min={min_v}, max={max_v}, clamp={clamp_v}, pow={pow_v}"
    );
    Ok(())
}

declare_plugin!(
    MathExamplePlugin,
    "example.math_ops",
    [
        src_i32, src_f64, sink_i32, sink_f64, add, sub, mul, div, rem, min, max, clamp, pow
    ]
);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut reg = PluginRegistry::new();
    let example = MathExamplePlugin::new();

    reg.register_capability_typed::<i32, _>("Add", |a, b| Ok(*a + *b));
    reg.register_capability_typed::<f64, _>("Add", |a, b| Ok(*a + *b));
    reg.register_capability_typed::<i32, _>("Sub", |a, b| Ok(*a - *b));
    reg.register_capability_typed::<f64, _>("Sub", |a, b| Ok(*a - *b));
    reg.register_capability_typed::<i32, _>("Mul", |a, b| Ok(*a * *b));
    reg.register_capability_typed::<f64, _>("Mul", |a, b| Ok(*a * *b));
    reg.register_capability_typed::<i32, _>("Div", |a, b| Ok(*a / *b));
    reg.register_capability_typed::<f64, _>("Div", |a, b| Ok(*a / *b));
    reg.register_capability_typed::<i32, _>("Rem", |a, b| Ok(*a % *b));
    reg.register_capability_typed::<f64, _>("Rem", |a, b| Ok(*a % *b));
    reg.register_capability_typed::<i32, _>("Min", |a, b| Ok((*a).min(*b)));
    reg.register_capability_typed::<f64, _>("Min", |a, b| Ok((*a).min(*b)));
    reg.register_capability_typed::<i32, _>("Max", |a, b| Ok((*a).max(*b)));
    reg.register_capability_typed::<f64, _>("Max", |a, b| Ok((*a).max(*b)));
    reg.register_capability_typed3::<i32, _>("Clamp", |x, lo, hi| Ok((*x).clamp(*lo, *hi)));
    reg.register_capability_typed3::<f64, _>("Clamp", |x, lo, hi| Ok((*x).clamp(*lo, *hi)));
    reg.register_capability_typed::<i32, _>("Pow", |a, b| Ok(a.pow((*b).max(0) as u32)));
    reg.register_capability_typed::<f64, _>("Pow", |a, b| Ok(a.powf(*b)));
    reg.install_plugin(&example)?;
    let handlers = reg.take_handlers();

    // I32 flow handles.
    let src_i32_handle = example.src_i32.alias("i32_src");
    let add_i32 = example.add.clone().alias("add_i32");
    let sub_i32 = example.sub.clone().alias("sub_i32");
    let mul_i32 = example.mul.clone().alias("mul_i32");
    let div_i32 = example.div.clone().alias("div_i32");
    let rem_i32 = example.rem.clone().alias("rem_i32");
    let max_i32 = example.max.clone().alias("max_i32");
    let min_i32 = example.min.clone().alias("min_i32");
    let clamp_i32 = example.clamp.clone().alias("clamp_i32");
    let pow_i32 = example.pow.clone().alias("pow_i32");
    let sink_i32_handle = example.sink_i32.alias("i32_sink");

    // F64 flow handles.
    let src_f64_handle = example.src_f64.alias("f64_src");
    let add_f64 = example.add.clone().alias("add_f64");
    let sub_f64 = example.sub.clone().alias("sub_f64");
    let mul_f64 = example.mul.clone().alias("mul_f64");
    let div_f64 = example.div.clone().alias("div_f64");
    let rem_f64 = example.rem.clone().alias("rem_f64");
    let max_f64 = example.max.clone().alias("max_f64");
    let min_f64 = example.min.clone().alias("min_f64");
    let clamp_f64 = example.clamp.clone().alias("clamp_f64");
    let pow_f64 = example.pow.clone().alias("pow_f64");
    let sink_f64_handle = example.sink_f64.alias("f64_sink");

    let graph = GraphBuilder::new(&reg.registry)
        .node(&src_i32_handle)
        .node(&src_f64_handle)
        .node(&add_i32)
        .node(&sub_i32)
        .node(&mul_i32)
        .node(&div_i32)
        .node(&rem_i32)
        .node(&max_i32)
        .node(&min_i32)
        .node(&clamp_i32)
        .node(&pow_i32)
        .node(&sink_i32_handle)
        .node(&add_f64)
        .node(&sub_f64)
        .node(&mul_f64)
        .node(&div_f64)
        .node(&rem_f64)
        .node(&max_f64)
        .node(&min_f64)
        .node(&clamp_f64)
        .node(&pow_f64)
        .node(&sink_f64_handle)
        // Wire i32 pipeline.
        .connect(&src_i32_handle.outputs.a, &add_i32.inputs.a)
        .connect(&src_i32_handle.outputs.b, &add_i32.inputs.b)
        .connect(&src_i32_handle.outputs.a, &sub_i32.inputs.a)
        .connect(&src_i32_handle.outputs.b, &sub_i32.inputs.b)
        .connect(&src_i32_handle.outputs.a, &mul_i32.inputs.a)
        .connect(&src_i32_handle.outputs.b, &mul_i32.inputs.b)
        .connect(&src_i32_handle.outputs.a, &div_i32.inputs.a)
        .connect(&src_i32_handle.outputs.b, &div_i32.inputs.b)
        .connect(&src_i32_handle.outputs.a, &rem_i32.inputs.a)
        .connect(&src_i32_handle.outputs.b, &rem_i32.inputs.b)
        .connect(&src_i32_handle.outputs.a, &max_i32.inputs.a)
        .connect(&src_i32_handle.outputs.b, &max_i32.inputs.b)
        .connect(&src_i32_handle.outputs.a, &min_i32.inputs.a)
        .connect(&src_i32_handle.outputs.b, &min_i32.inputs.b)
        .connect(&src_i32_handle.outputs.a, &clamp_i32.inputs.x)
        .connect(&src_i32_handle.outputs.b, &clamp_i32.inputs.lo)
        .connect(&src_i32_handle.outputs.a, &clamp_i32.inputs.hi)
        .connect(&src_i32_handle.outputs.a, &pow_i32.inputs.base)
        .connect(&src_i32_handle.outputs.b, &pow_i32.inputs.exp)
        .connect(&add_i32.outputs.out, &sink_i32_handle.inputs.sum)
        .connect(&sub_i32.outputs.out, &sink_i32_handle.inputs.diff)
        .connect(&mul_i32.outputs.out, &sink_i32_handle.inputs.product)
        .connect(&div_i32.outputs.out, &sink_i32_handle.inputs.quot)
        .connect(&rem_i32.outputs.out, &sink_i32_handle.inputs.rem)
        .connect(&min_i32.outputs.out, &sink_i32_handle.inputs.min)
        .connect(&max_i32.outputs.out, &sink_i32_handle.inputs.max)
        .connect(&clamp_i32.outputs.out, &sink_i32_handle.inputs.clamp)
        .connect(&pow_i32.outputs.out, &sink_i32_handle.inputs.pow)
        // Wire f64 pipeline.
        .connect(&src_f64_handle.outputs.a, &add_f64.inputs.a)
        .connect(&src_f64_handle.outputs.b, &add_f64.inputs.b)
        .connect(&src_f64_handle.outputs.a, &sub_f64.inputs.a)
        .connect(&src_f64_handle.outputs.b, &sub_f64.inputs.b)
        .connect(&src_f64_handle.outputs.a, &mul_f64.inputs.a)
        .connect(&src_f64_handle.outputs.b, &mul_f64.inputs.b)
        .connect(&src_f64_handle.outputs.a, &div_f64.inputs.a)
        .connect(&src_f64_handle.outputs.b, &div_f64.inputs.b)
        .connect(&src_f64_handle.outputs.a, &rem_f64.inputs.a)
        .connect(&src_f64_handle.outputs.b, &rem_f64.inputs.b)
        .connect(&src_f64_handle.outputs.a, &max_f64.inputs.a)
        .connect(&src_f64_handle.outputs.b, &max_f64.inputs.b)
        .connect(&src_f64_handle.outputs.a, &min_f64.inputs.a)
        .connect(&src_f64_handle.outputs.b, &min_f64.inputs.b)
        .connect(&src_f64_handle.outputs.a, &clamp_f64.inputs.x)
        .connect(&src_f64_handle.outputs.b, &clamp_f64.inputs.lo)
        .connect(&src_f64_handle.outputs.a, &clamp_f64.inputs.hi)
        .connect(&src_f64_handle.outputs.a, &pow_f64.inputs.base)
        .connect(&src_f64_handle.outputs.b, &pow_f64.inputs.exp)
        .connect(&add_f64.outputs.out, &sink_f64_handle.inputs.sum)
        .connect(&sub_f64.outputs.out, &sink_f64_handle.inputs.diff)
        .connect(&mul_f64.outputs.out, &sink_f64_handle.inputs.product)
        .connect(&div_f64.outputs.out, &sink_f64_handle.inputs.quot)
        .connect(&rem_f64.outputs.out, &sink_f64_handle.inputs.rem)
        .connect(&min_f64.outputs.out, &sink_f64_handle.inputs.min)
        .connect(&max_f64.outputs.out, &sink_f64_handle.inputs.max)
        .connect(&clamp_f64.outputs.out, &sink_f64_handle.inputs.clamp)
        .connect(&pow_f64.outputs.out, &sink_f64_handle.inputs.pow)
        .build();

    let mut cfg = EngineConfig::default();
    cfg.runtime.mode = RuntimeMode::Parallel;
    cfg.runtime.pool_size = None;
    cfg.runtime.default_policy = EdgePolicyKind::Fifo;
    cfg.runtime.backpressure = BackpressureStrategy::None;
    cfg.gpu = GpuBackend::Cpu;
    let engine = Engine::new(cfg)?;

    let result = engine.run(&reg.registry, graph, handlers)?;
    println!("telemetry: {:?}", result.telemetry);
    Ok(())
}

#[cfg(not(all(feature = "engine", feature = "plugins")))]
fn main() {
    eprintln!("enable `engine` and `plugins` features to run this example");
}
