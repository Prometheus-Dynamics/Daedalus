//! What a plan is checked against. Every refusal has a plan that causes it.

#![cfg(feature = "blob")]

#[path = "shared/blob_fixture.rs"]
mod fixture;

use daedalus_cpu::blob::{
    ConstDesc, Interpreter, Library, LoadError, ParamDesc, PlanDesc, ScalarKind, Source, encode,
};
use daedalus_plan::crc32;
use daedalus_registry::NodeRecord;
use fixture::{BAD_NODE, GAIN_NODE, HEAVY, LIBRARY, OPT, SUM_NODE, plan};

type Small = Interpreter<1024>;

fn loading(plan: &PlanDesc) -> Result<(), LoadError> {
    let bytes = encode(plan).unwrap();
    let mut running = Box::new(Small::new());
    running.load(&LIBRARY, &bytes)
}

#[test]
fn the_fixture_is_a_valid_plan() {
    assert_eq!(loading(&plan()), Ok(()));
}

#[test]
fn the_same_description_gives_the_same_bytes_and_names_its_library() {
    let (one, two) = (encode(&plan()).unwrap(), encode(&plan()).unwrap());
    assert_eq!(one, two);
    let mut other = plan();
    other.library ^= 1;
    assert_ne!(one, encode(&other).unwrap());
}

#[test]
fn a_library_hash_depends_on_interfaces_and_their_order() {
    const FIRST: &[&NodeRecord; 2] = &[&GAIN_NODE, &SUM_NODE];
    const SWAPPED: &[&NodeRecord; 2] = &[&SUM_NODE, &GAIN_NODE];
    const OTHER: &[&NodeRecord; 2] = &[&GAIN_NODE, &BAD_NODE];
    let hash = |nodes: &'static [&'static NodeRecord; 2]| Library::new(nodes).hash;
    assert_eq!(hash(FIRST), hash(FIRST));
    assert_ne!(hash(FIRST), hash(SWAPPED));
    assert_ne!(hash(FIRST), hash(OTHER));
}

#[test]
fn not_a_plan_is_malformed() {
    let mut boxed = Box::new(Small::new());
    assert_eq!(boxed.load(&LIBRARY, b"nonsense"), Err(LoadError::Malformed));
    assert_eq!(boxed.load(&LIBRARY, b""), Err(LoadError::Malformed));
}

#[test]
fn a_cut_off_plan_is_refused() {
    let bytes = encode(&plan()).unwrap();
    let mut boxed = Box::new(Small::new());
    assert!(matches!(
        boxed.load(&LIBRARY, &bytes[..bytes.len() - 9]),
        Err(LoadError::Checksum { .. })
    ));
}

#[test]
fn another_format_is_refused_whatever_its_checksum() {
    let mut bytes = encode(&plan()).unwrap();
    bytes[8] = 0xee;
    let mut boxed = Box::new(Small::new());
    assert!(matches!(
        boxed.load(&LIBRARY, &bytes),
        Err(LoadError::Version { .. })
    ));
}

#[test]
fn a_flipped_bit_fails_the_checksum() {
    let mut bytes = encode(&plan()).unwrap();
    let at = bytes.len() / 2;
    bytes[at] ^= 1;
    let mut boxed = Box::new(Small::new());
    assert!(matches!(
        boxed.load(&LIBRARY, &bytes),
        Err(LoadError::Checksum { .. })
    ));
}

#[test]
fn another_librarys_plan_is_refused() {
    let mut other = plan();
    other.library ^= 1;
    assert!(matches!(loading(&other), Err(LoadError::Library { .. })));
}

#[test]
fn unknown_entries_and_wrong_port_counts_name_the_node() {
    let mut unknown = plan();
    unknown.steps[1].entry = 99;
    assert_eq!(loading(&unknown), Err(LoadError::Node { node: 1 }));

    let mut extra = plan();
    extra.steps[0].inputs.push(Source::Absent);
    assert_eq!(loading(&extra), Err(LoadError::Node { node: 0 }));

    let mut short = plan();
    short.steps[1].inputs.clear();
    assert_eq!(loading(&short), Err(LoadError::Node { node: 1 }));

    // Its output is a String, which can't live in a queue here.
    let mut heavy = plan();
    heavy.steps[1].entry = HEAVY;
    assert_eq!(loading(&heavy), Err(LoadError::Node { node: 1 }));
}

#[test]
fn a_required_input_needs_a_source_and_a_fitting_constant() {
    let mut absent = plan();
    absent.steps[0].inputs[1] = Source::Absent;
    assert_eq!(
        loading(&absent),
        Err(LoadError::Input { node: 0, input: 1 })
    );

    let mut mistyped = plan();
    mistyped.consts[0].kind = ScalarKind::I32;
    assert_eq!(
        loading(&mistyped),
        Err(LoadError::Input { node: 0, input: 1 })
    );

    let mut missing = plan();
    missing.steps[0].inputs[1] = Source::Const(5);
    assert_eq!(
        loading(&missing),
        Err(LoadError::Input { node: 0, input: 1 })
    );
}

#[test]
fn an_optional_input_can_be_left_empty() {
    let mut empty = plan();
    empty.steps[1].entry = OPT;
    empty.steps[1].inputs[0] = Source::Absent;
    empty.edges.remove(1);
    empty.steps[0].outputs.clear();
    empty.steps[1].outputs = vec![(0, 1)];
    empty.host_outputs[0].edges = vec![1];
    assert_eq!(loading(&empty), Ok(()));
}

#[test]
fn edges_need_one_producer_and_one_consumer_in_order() {
    let mut out_of_range = plan();
    out_of_range.steps[1].inputs[0] = Source::Edge(9);
    assert_eq!(loading(&out_of_range), Err(LoadError::Edge { edge: 9 }));

    let mut orphan = plan();
    orphan.steps[0].outputs.clear();
    assert_eq!(loading(&orphan), Err(LoadError::Edge { edge: 1 }));

    let mut backwards = plan();
    backwards.steps.swap(0, 1);
    assert_eq!(loading(&backwards), Err(LoadError::Edge { edge: 1 }));

    let mut unbuffered = plan();
    unbuffered.edges[0].capacity = 0;
    assert_eq!(loading(&unbuffered), Err(LoadError::Edge { edge: 0 }));

    let mut two_readers = plan();
    two_readers.steps[1].inputs[0] = Source::Edge(1);
    two_readers.steps[0].inputs[1] = Source::Edge(1);
    assert_eq!(loading(&two_readers), Err(LoadError::Edge { edge: 1 }));
}

#[test]
fn the_ends_of_an_edge_must_agree() {
    let mut key = plan();
    key.edges[1].key ^= 1;
    assert_eq!(loading(&key), Err(LoadError::Type { edge: 1 }));

    let mut size = plan();
    size.edges[1].size = 8;
    assert_eq!(loading(&size), Err(LoadError::Type { edge: 1 }));
}

#[test]
fn parameters_must_sit_inside_their_range() {
    let parameter = |value, min, max| ParamDesc { value, min, max };
    let mut fine = plan();
    fine.params.push(parameter(0, 1, 5));
    assert_eq!(loading(&fine), Ok(()));

    let mut above = plan();
    above.params.push(parameter(0, 0, 2));
    assert_eq!(loading(&above), Err(LoadError::Param { param: 0 }));

    let mut unknown = plan();
    unknown.params.push(parameter(4, 0, 9));
    assert_eq!(loading(&unknown), Err(LoadError::Param { param: 0 }));

    let mut negative = plan();
    negative.consts[0] = ConstDesc {
        kind: ScalarKind::I32,
        bits: (-3_i32) as u32 as u64,
    };
    negative.steps[0].inputs[1] = Source::Absent;
    negative.steps[0].entry = OPT;
    negative.steps[0].inputs = vec![Source::Edge(0)];
    negative
        .params
        .push(parameter(0, (-5_i32) as u32 as u64, 0));
    assert_eq!(loading(&negative), Ok(()));

    let mut nan = plan();
    nan.consts.push(ConstDesc {
        kind: ScalarKind::F32,
        bits: u64::from(f32::NAN.to_bits()),
    });
    nan.params.push(parameter(
        1,
        u64::from(0.0_f32.to_bits()),
        u64::from(1.0_f32.to_bits()),
    ));
    assert_eq!(loading(&nan), Err(LoadError::Param { param: 0 }));
}

#[test]
fn host_ports_must_match_their_edges() {
    let mut mistyped = plan();
    mistyped.host_inputs[0].key ^= 1;
    assert_eq!(loading(&mistyped), Err(LoadError::Port { port: 0 }));

    let mut output = plan();
    output.host_outputs[0].key ^= 1;
    assert_eq!(loading(&output), Err(LoadError::Port { port: 0 }));
}

#[test]
fn a_plan_too_big_for_the_arena_says_how_much_it_needs() {
    let bytes = encode(&plan()).unwrap();
    let needed = Interpreter::<16>::required(&LIBRARY, &bytes).unwrap();
    let mut tiny = Box::new(Interpreter::<16>::new());
    assert_eq!(
        tiny.load(&LIBRARY, &bytes),
        Err(LoadError::Arena {
            needed: needed as u32
        })
    );
    let mut huge = plan();
    huge.edges[0].capacity = 60_000;
    huge.edges[0].size = 4;
    assert!(matches!(loading(&huge), Err(LoadError::Arena { .. })));
}

#[test]
fn a_checksum_is_over_every_byte_before_it() {
    let bytes = encode(&plan()).unwrap();
    let (body, trailer) = bytes.split_at(bytes.len() - 4);
    assert_eq!(crc32(body).to_le_bytes(), trailer);
}
