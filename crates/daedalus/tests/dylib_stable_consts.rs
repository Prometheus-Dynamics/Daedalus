//! Const inputs on the stable handler path: a plugin instance receives its consts once (on its
//! first call) and again when a graph patch changes them, so a patched const moves the result
//! exactly as it does on the static install, for scalar and string consts
//! (`examples/plugins/stable_bench`: `config_s8`'s `c0` is an `i64`, `config_m8`'s `c3` a
//! `String` whose length is in the sum).
// musl executables are static and cannot dlopen (see `dylib::library`): no loader there.
#![cfg(all(unix, not(target_env = "musl")))]

// The shared cases; this test uses the config nodes only.
#[allow(dead_code)]
#[path = "support/stable_bench.rs"]
mod bench;
#[path = "support/cdylib.rs"]
mod cdylib;
#[path = "support/frames.rs"]
mod frames;

use std::borrow::Cow;

use daedalus::PluginLibrary;
use daedalus::data::model::Value;
use daedalus::planner::{GraphNodeSelector, GraphPatch, GraphPatchOp};
use daedalus::runtime::plugins::PluginRegistry;

use bench::{Host, Step};

/// The config node's index in the compiled graph (`["host", "cfg"]`).
const CONFIG_NODE: usize = 1;

/// A patch setting one const of the config node.
fn set_const(port: &str, value: Value) -> GraphPatch {
    GraphPatch {
        version: 1,
        ops: vec![GraphPatchOp::SetNodeConst {
            node: GraphNodeSelector {
                index: Some(CONFIG_NODE),
                ..GraphNodeSelector::default()
            },
            port: port.into(),
            value: Some(value),
        }],
    }
}

/// Ticks `host` once, returning the config node's output.
fn tick(host: &mut Host, step: &mut Step) -> i64 {
    step(host)
}

/// Checks the patch on both installs: the output of the tick after it moves by `delta` (one
/// tick plus the constant's change) on each, and both agree on every tick.
fn check_patch(
    statics: &PluginRegistry,
    stables: &PluginRegistry,
    mixed: bool,
    consts: usize,
    patch: GraphPatch,
    delta: i64,
) {
    let (mut want, mut want_step) = bench::config_case(statics, mixed, consts);
    let (mut got, mut got_step) = bench::config_case(stables, mixed, consts);
    for _ in 0..3 {
        assert_eq!(
            tick(&mut want, &mut want_step),
            tick(&mut got, &mut got_step)
        );
    }
    let (want_before, got_before) = (
        tick(&mut want, &mut want_step),
        tick(&mut got, &mut got_step),
    );
    assert_eq!(want_before, got_before);
    assert_eq!(want.apply_patch(&patch).applied_ops, 1);
    assert_eq!(got.apply_patch(&patch).applied_ops, 1);
    let (want_after, got_after) = (
        tick(&mut want, &mut want_step),
        tick(&mut got, &mut got_step),
    );
    assert_eq!(
        want_after - want_before,
        delta,
        "the static install applies the patch"
    );
    assert_eq!(
        got_after - got_before,
        delta,
        "the stable instance receives the patch"
    );
    for _ in 0..3 {
        assert_eq!(
            tick(&mut want, &mut want_step),
            tick(&mut got, &mut got_step)
        );
    }
}

#[test]
fn patched_consts_reach_a_stable_instance_like_the_static_install() {
    let path = cdylib::build_cdylib(
        "daedalus-plugins-stable-bench",
        "daedalus_plugins_stable_bench",
        "dylib",
    );
    // Safety: a trusted test plugin, kept loaded for the process.
    let library: &'static PluginLibrary =
        Box::leak(Box::new(unsafe { PluginLibrary::load(&path) }.unwrap()));
    let (statics, stables) = (bench::static_registry(), bench::stable_registry(library));

    // `c0` was 3 (`7 * 0 + 3`); the tick adds 1 and the constant moves to 1000.
    check_patch(
        &statics,
        &stables,
        false,
        8,
        set_const("c0", Value::Int(1000)),
        1 + (1000 - 3),
    );
    // `c3` was "enum_variant_name" (17 bytes); the tick adds 1 and the length moves to 31.
    let longer = Value::String(Cow::Borrowed("a_much_longer_enum_variant_name"));
    check_patch(
        &statics,
        &stables,
        true,
        8,
        set_const("c3", longer),
        1 + (31 - 17),
    );
}

/// A stable node with 70 const inputs: the list is over the shared cap (64), so the consts
/// spill into the call's inputs. The last one (`c69`, an `i64`, `69 * 7 + 3` = 486) must read
/// as it does on the static install: patching it moves the output by the same amount on both.
#[test]
fn the_last_const_of_a_stable_node_over_the_shared_cap_is_read() {
    let path = cdylib::build_cdylib(
        "daedalus-plugins-stable-bench",
        "daedalus_plugins_stable_bench",
        "dylib",
    );
    // Safety: a trusted test plugin, kept loaded for the process.
    let library: &'static PluginLibrary =
        Box::leak(Box::new(unsafe { PluginLibrary::load(&path) }.unwrap()));
    let (statics, stables) = (bench::static_registry(), bench::stable_registry(library));
    check_patch(
        &statics,
        &stables,
        false,
        70,
        set_const("c69", Value::Int(1000)),
        1 + (1000 - (69 * 7 + 3)),
    );
}
