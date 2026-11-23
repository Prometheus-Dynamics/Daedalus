//! A node's output names in the stored format: added at the end of the node's record, so a plan
//! stored before them still reads, with no names.

use std::collections::BTreeMap;

use daedalus_wire::codec::{Reader, Writer};

use crate::decode::read_node;
use crate::encode::{write_node, write_node_as_first_stored};
use crate::facts::{MadeMetric, MadeNode};
use daedalus_registry::MetricUnit;

fn node() -> MadeNode {
    MadeNode {
        id: "t.pair".into(),
        label: None,
        bridge: false,
        metadata: BTreeMap::new(),
        fire_all: true,
        required: vec!["in".into()],
        reusable: false,
        inputs: vec!["in".into(), "cal".into()],
        consts: BTreeMap::new(),
        backend: Some(0),
        outputs: vec!["out".into(), "spare".into()],
        metrics: vec![
            MadeMetric {
                name: "threshold".into(),
                unit: MetricUnit::Ns,
            },
            MadeMetric {
                name: "candidates".into(),
                unit: MetricUnit::Count,
            },
        ],
    }
}

fn read(bytes: &[u8]) -> MadeNode {
    let mut reader = Reader::new(bytes);
    let node = reader.record(read_node).unwrap();
    reader.finish().unwrap();
    node
}

#[test]
fn a_node_keeps_its_output_names_and_its_metrics() {
    let mut w = Writer::new();
    w.record(|w| write_node(w, &node())).unwrap();
    assert_eq!(read(&w.buf), node());
}

#[test]
fn a_plan_stored_before_the_output_names_and_metrics_reads_a_node_with_neither() {
    let mut w = Writer::new();
    w.record(|w| write_node_as_first_stored(w, &node()))
        .unwrap();
    let expected = MadeNode {
        outputs: Vec::new(),
        metrics: Vec::new(),
        ..node()
    };
    assert_eq!(read(&w.buf), expected);
}
