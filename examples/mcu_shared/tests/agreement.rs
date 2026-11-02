//! The full runtime and the device describe each shared node the same way: the `NodeDecl` the
//! runtime's `#[node]` declares equals what `daedalus-mcu-build` derives from the device's
//! `NodeDesc`, on everything the device models (node id, port names, type keys, schemas,
//! optional inputs, conditional outputs, fire mode), and the state types start from the same
//! value in both (`Default` is the runtime's initial state, `NodeState::INIT` the device's).
#![cfg(feature = "runtime")]

use daedalus::{
    data::model::TypeExpr,
    registry::capability::{NodeDecl, PortDecl},
    runtime::NODE_FIRE_META_KEY,
};
use daedalus_mcu::{NodeDesc, NodeState};
use daedalus_mcu_build::{NodeSpec, node_decl};
use daedalus_mcu_shared::{
    ClampNode, Edges, Lowpass, LowpassNode, RisingNode, ScaleNode, ThresholdNode, clamp, lowpass,
    rising, scale, threshold,
};

/// The parts of a declaration the device models, in declaration order.
#[derive(Debug, PartialEq)]
struct Contract {
    id: String,
    fire: Option<String>,
    inputs: Vec<Port>,
    outputs: Vec<(Port, Option<String>)>,
}

#[derive(Debug, PartialEq)]
struct Port {
    name: String,
    key: daedalus::transport::TypeKey,
    schema: Option<TypeExpr>,
    optional: bool,
}

fn port(port: &PortDecl) -> Port {
    Port {
        name: port.name.clone(),
        key: port.type_key.clone(),
        schema: port.schema.clone(),
        optional: port.optional,
    }
}

fn contract(decl: &NodeDecl) -> Contract {
    let conditional = |name: &str| {
        decl.metadata_json
            .get(&format!("outputs.{name}.conditional"))
            .cloned()
    };
    Contract {
        id: decl.id.to_string(),
        fire: decl.metadata_json.get(NODE_FIRE_META_KEY).cloned(),
        inputs: decl.inputs.iter().map(port).collect(),
        outputs: decl
            .outputs
            .iter()
            .map(|out| (port(out), conditional(&out.name)))
            .collect(),
    }
}

fn device(desc: NodeDesc) -> Contract {
    contract(&node_decl(&NodeSpec::from(&desc)))
}

#[test]
fn runtime_declarations_match_the_device_declarations() {
    let pairs = [
        (
            contract(&ScaleNode::node_decl().expect("scale")),
            device(scale::NODE),
        ),
        (
            contract(&LowpassNode::node_decl().expect("lowpass")),
            device(lowpass::NODE),
        ),
        (
            contract(&ClampNode::node_decl().expect("clamp")),
            device(clamp::NODE),
        ),
        (
            contract(&ThresholdNode::node_decl().expect("threshold")),
            device(threshold::NODE),
        ),
        (
            contract(&RisingNode::node_decl().expect("rising")),
            device(rising::NODE),
        ),
    ];
    for (runtime, device) in pairs {
        assert_eq!(runtime, device, "runtime vs device declaration");
    }
}

#[test]
fn declarations_are_not_vacuous() {
    let clamp = device(clamp::NODE);
    assert_eq!(clamp.id, "shared:clamp");
    assert!(clamp.inputs[1].optional, "ceiling is an optional input");
    let rising = device(rising::NODE);
    assert_eq!(
        rising.outputs[0].1.as_deref(),
        Some(r#"{"type":"Bool","value":true}"#),
        "count is a conditional output"
    );
}

#[test]
fn state_starts_from_the_same_value() {
    assert_eq!(Lowpass::default(), <Lowpass as NodeState>::INIT);
    assert_eq!(Edges::default(), <Edges as NodeState>::INIT);
    // The shared functions take the same state types in both builds.
    let mut state = Lowpass::INIT;
    assert_eq!(lowpass(1.0, 0.5, &mut state), 1.0);
}
