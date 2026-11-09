//! Payload inspection through the serializer map, and `push_value`.

use std::sync::Arc;

use daedalus_abi::{Slots, Step, StepCx, StepEnd, StepWord};
use daedalus_host::{Dir, Host, HostError, HostValue, PortSpec, Ports, ValueSerializers};
use daedalus_plan::Span;
use daedalus_runner::{Memory, Plan, Recovery, Runner, SlotKind, SlotSpec, StepMeta};
use daedalus_types::{SpaceIndex, TypeIndex, TypeKey};
use daedalus_value::{Owner, Value};

const TY: TypeIndex = TypeIndex(0);
const SP: SpaceIndex = SpaceIndex(0);
static KEY: TypeKey = TypeKey::new("demo.int");
static OTHER: TypeKey = TypeKey::new("demo.other");
static TYPES: [TypeKey; 2] = [TypeKey::new("demo.int"), TypeKey::new("demo.other")];

struct Idle;

impl StepWord for Idle {
    fn run(_: usize, _: &mut Slots, _: &mut StepCx) -> StepEnd {
        StepEnd::Done
    }
}

fn leak<T>(values: Vec<T>) -> &'static mut [T] {
    Box::leak(values.into_boxed_slice())
}

fn host() -> Host<'static> {
    let specs: &'static [SlotSpec] = leak(vec![SlotSpec {
        cap: 4,
        kind: SlotKind::Fifo,
    }]);
    let span = Span { start: 0, len: 0 };
    let meta: &'static [StepMeta] = leak(vec![StepMeta {
        wake: span,
        inputs: span,
        outputs: span,
        always: false,
        may_pend: false,
        recovery: Recovery::Fail,
        fire: daedalus_runner::Fire::Any,
        required: span,
        reuse: false,
    }]);
    let memory = Memory {
        cells: leak(vec![daedalus_abi::Slot::EMPTY; 1]),
        queues: leak(vec![daedalus_runner::Queue::default(); 1]),
        storage: leak(vec![daedalus_abi::Slot::EMPTY; 4]),
        dirty: leak(vec![0; 1]),
        carry: leak(vec![0; 1]),
        always: leak(vec![0; 1]),
        pending: leak(Vec::new()),
        health: leak(vec![daedalus_runner::StepHealth::default(); 1]),
        activity: leak(vec![0; 1]),
        inline: leak(vec![[0u8; 16]; 4]),
    };
    let runner = Runner::load(
        Plan {
            steps: leak(vec![Step::of::<Idle>(0)]),
            meta,
            table: &[],
            slots: specs,
        },
        memory,
    )
    .unwrap();
    let ports: &'static [PortSpec<'static>] = leak(vec![PortSpec {
        name: "in",
        dir: Dir::Input,
        slot: 0,
        ty: TY,
        space: SP,
        key: &KEY,
        wake: &[],
    }]);
    Host::new(runner, Ports::new(ports).unwrap(), &TYPES)
}

fn int_payload(value: i64) -> Value {
    Value {
        ty: TY,
        space: SP,
        owner: Owner::Shared(Arc::from(&value.to_le_bytes()[..])),
    }
}

fn serializers() -> ValueSerializers {
    let mut map = ValueSerializers::new();
    map.insert_both_for_key(
        &KEY,
        Box::new(|value: &Value| {
            let bytes = <[u8; 8]>::try_from(value.bytes()?).ok()?;
            Some(HostValue::Int(i64::from_le_bytes(bytes)))
        }),
        Box::new(|value: &HostValue| match value {
            HostValue::Int(number) => Some(int_payload(*number)),
            _ => None,
        }),
    );
    map
}

#[test]
fn inspect_uses_the_serializer_for_the_type_key() {
    let host = host();
    let inspected = host.inspect_payload(&int_payload(42), &serializers());
    assert_eq!(inspected.value, Some(HostValue::Int(42)));
    assert_eq!(inspected.summary.type_key, "demo.int");
    assert_eq!(inspected.to_display_value(), HostValue::Int(42));
}

#[test]
fn an_unregistered_type_gives_a_summary_not_an_error() {
    let host = host();
    let payload = Value {
        ty: TypeIndex(1),
        space: SP,
        owner: Owner::Static(b"bytes"),
    };
    let inspected = host.inspect_payload(&payload, &serializers());
    assert_eq!(inspected.value, None);
    assert_eq!(inspected.summary.type_key, OTHER.as_str());
    assert_eq!(inspected.summary.bytes_estimate, Some(5));
    assert!(matches!(inspected.to_display_value(), HostValue::Struct(_)));
}

#[test]
fn push_value_reaches_the_port_typed() {
    let mut host = host();
    host.push_value("in", &HostValue::Int(7), &serializers())
        .unwrap();
    assert_eq!(host.runner().queue(0).unwrap().len, 1);
}

#[test]
fn a_wrong_shaped_value_is_refused_with_the_port_and_key() {
    let mut host = host();
    let refused = host
        .push_value("in", &HostValue::Text("no".into()), &serializers())
        .unwrap_err();
    assert!(matches!(refused, HostError::Refused { .. }));
    let text = refused.to_string();
    assert!(text.contains("in") && text.contains("demo.int"), "{text}");
    assert_eq!(host.runner().queue(0).unwrap().len, 0);
}
