//! A plugin's codecs convert its plain data to and from the host value form and the wire form.
//! Their conversions join the registry, and a codec keeps its library open.

mod support;

use daedalus_abi::{PlainData, layout_hash};
use daedalus_loader::{Direction, LoadError, Loader, PodBlock, PodError};
use daedalus_registry::Registry;
use daedalus_types::TypeKey;
use support::copy_fixture;

/// The host's view of the fixture's plain type: one `u32`, with the same key and layout the
/// fixture's derive gives it.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Number {
    value: u32,
}

// SAFETY: `Number` is `repr(C)`, one `u32` with no padding, and every bit pattern of it is valid.
unsafe impl PlainData for Number {
    const KEY: &'static str = "dylib.number";
    const LAYOUT: u64 = layout_hash(&[4, 4, 0]);
}

#[test]
fn a_value_is_converted_to_a_plugin_type_by_its_codec() {
    let mut loader = Loader::new();
    let plugin = loader.open(copy_fixture("codec-value")).expect("opens");
    let codec = &plugin.codecs()[0];
    assert_eq!(codec.key(), "dylib.number");
    assert_eq!(codec.layout(), Number::LAYOUT);
    // The host value 5 is the number 4.
    let number = codec
        .convert(Direction::FromValue, &5u32.to_ne_bytes())
        .expect("converts");
    assert_eq!(number, 4u32.to_ne_bytes());
    // And back: the number 4 is the host value 5.
    let value = codec
        .convert(Direction::ToValue, &number)
        .expect("converts back");
    assert_eq!(value, 5u32.to_ne_bytes());
}

#[test]
fn a_plugin_type_crosses_to_a_host_value_and_back_unchanged() {
    let mut loader = Loader::new();
    let plugin = loader.open(copy_fixture("codec-cross")).expect("opens");
    let codec = &plugin.codecs()[0];
    let start = Number { value: 41 };
    let block = PodBlock::of(&start);
    let bytes = codec
        .accept(&block)
        .expect("the block is this codec's type");
    let value = codec
        .convert(Direction::ToValue, bytes)
        .expect("to a value");
    let back = codec
        .convert(Direction::FromValue, &value)
        .expect("back to the plugin type");
    assert_eq!(back, start.value.to_ne_bytes());
    assert_eq!(block.read::<Number>().expect("reads"), start);
}

#[test]
fn a_codec_refuses_an_input_it_has_no_value_for() {
    let mut loader = Loader::new();
    let plugin = loader.open(copy_fixture("codec-refuse")).expect("opens");
    let codec = &plugin.codecs()[0];
    // Zero has no number below it.
    match codec.convert(Direction::FromValue, &0u32.to_ne_bytes()) {
        Err(error @ LoadError::Codec { .. }) => {
            assert!(error.to_string().contains("dylib.number"));
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}

#[test]
fn the_wire_form_is_big_endian_both_ways() {
    let mut loader = Loader::new();
    let plugin = loader.open(copy_fixture("codec-wire")).expect("opens");
    let codec = &plugin.codecs()[0];
    let wire = codec
        .convert(Direction::ToBytes, &7u32.to_ne_bytes())
        .expect("to the wire");
    assert_eq!(wire, [0, 0, 0, 7]);
    let back = codec
        .convert(Direction::FromBytes, &wire)
        .expect("from the wire");
    assert_eq!(back, 7u32.to_ne_bytes());
}

#[test]
fn the_four_conversions_join_the_registry_as_conversion_nodes() {
    let mut loader = Loader::new();
    let plugin = loader.open(copy_fixture("codec-join")).expect("opens");
    let mut registry = Registry::new();
    plugin.join(&mut registry).expect("joins");
    let conversions = registry.conversions();
    let find = |id: &str| {
        conversions
            .iter()
            .find(|entry| entry.id == id)
            .unwrap_or_else(|| panic!("no conversion {id}"))
    };
    let to_value = find("dylib.number.to_value");
    assert_eq!(to_value.from, TypeKey::shared("dylib.number").expect("key"));
    assert_eq!(to_value.to, TypeKey::shared("u32").expect("key"));
    assert_eq!((to_value.cost, to_value.lossy), (1, false));
    let from_value = find("dylib.number.from_value");
    assert_eq!(from_value.from, TypeKey::shared("u32").expect("key"));
    assert_eq!(from_value.to, TypeKey::shared("dylib.number").expect("key"));
    let to_bytes = find("dylib.number.to_bytes");
    assert_eq!(to_bytes.to, TypeKey::shared("bytes").expect("key"));
    let from_bytes = find("dylib.number.from_bytes");
    assert_eq!(from_bytes.from, TypeKey::shared("bytes").expect("key"));
    // The plugin's nodes are still the only nodes it declares.
    assert_eq!(plugin.nodes().len(), 5);
}

#[test]
fn a_codec_keeps_its_library_open_until_it_is_dropped() {
    let mut loader = Loader::new();
    let plugin = loader.open(copy_fixture("codec-hold")).expect("opens");
    let probe = plugin.generation_probe();
    let codec = plugin.codecs()[0].clone();
    drop(plugin);
    assert!(
        probe.upgrade().is_some(),
        "the codec holds the library open"
    );
    let value = codec
        .convert(Direction::ToValue, &9u32.to_ne_bytes())
        .expect("still runs");
    assert_eq!(value, 10u32.to_ne_bytes());
    drop(codec);
    assert!(
        probe.upgrade().is_none(),
        "the library closes with its last codec"
    );
}

#[test]
fn a_block_of_another_layout_under_the_codec_key_is_refused_naming_both() {
    let mut loader = Loader::new();
    let plugin = loader.open(copy_fixture("codec-layout")).expect("opens");
    let codec = &plugin.codecs()[0];
    let other = PodBlock::new("dylib.number", codec.layout() ^ 1, &4u32.to_ne_bytes());
    match codec.accept(&other) {
        Err(error @ PodError::Layout { .. }) => {
            let text = error.to_string();
            assert!(text.contains(&format!("{:#018x}", codec.layout())));
            assert!(text.contains(&format!("{:#018x}", codec.layout() ^ 1)));
        }
        other => panic!("expected a layout refusal, got {other:?}"),
    }
    let same = codec.block(&4u32.to_ne_bytes());
    assert!(codec.accept(&same).is_ok());
}

#[test]
fn a_block_read_as_a_type_of_another_size_is_refused() {
    let short = PodBlock::new("dylib.number", Number::LAYOUT, &[1, 2]);
    match short.read::<Number>() {
        Err(PodError::Size {
            expected: 4,
            found: 2,
            ..
        }) => {}
        other => panic!("expected a size refusal, got {other:?}"),
    }
}
