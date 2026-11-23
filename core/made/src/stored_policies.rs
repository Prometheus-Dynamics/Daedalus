//! The host inputs' policies in the stored format: added at the end of the stitched record, so a
//! plan stored before them still reads, with every input `Consume`.

use daedalus_graph::HostInputPolicy;
use daedalus_plan::PlanData;
use daedalus_wire::WireError;
use daedalus_wire::codec::{Reader, Writer};

use crate::decode::read_stitched;
use crate::encode::{write_input_policies, write_stitched, write_stitched_as_first_stored};
use crate::stitched::{HostPort, SlotInfo, Stitched};

fn port(name: &str, slot: u32, policy: HostInputPolicy) -> HostPort {
    HostPort {
        node: 0,
        port: name.into(),
        slot,
        policy,
    }
}

fn stitched() -> Stitched {
    let slot = SlotInfo {
        space: 0,
        ty: Some(0),
        consumers: 1,
    };
    Stitched {
        layout: Vec::new(),
        slots: vec![slot; 4],
        spaces: vec!["host".into()],
        transfers: Vec::new(),
        host_inputs: vec![
            port("cal", 0, HostInputPolicy::Held),
            port("shared", 1, HostInputPolicy::Shared),
            port("x", 2, HostInputPolicy::Consume),
        ],
        host_outputs: vec![port("y", 3, HostInputPolicy::Consume)],
        plan: PlanData::default(),
    }
}

fn read(bytes: &[u8]) -> Result<Stitched, WireError> {
    let mut reader = Reader::new(bytes);
    let stitched = reader.record(read_stitched)?;
    reader.finish()?;
    Ok(stitched)
}

#[test]
fn each_host_input_keeps_its_policy() {
    let mut w = Writer::new();
    w.record(|w| write_stitched(w, &stitched())).unwrap();
    assert_eq!(read(&w.buf).unwrap(), stitched());
}

#[test]
fn a_plan_stored_before_the_policies_reads_every_input_as_consume() {
    let mut w = Writer::new();
    w.record(|w| write_stitched_as_first_stored(w, &stitched()))
        .unwrap();
    let back = read(&w.buf).unwrap();
    let policies: Vec<_> = back.host_inputs.iter().map(|port| port.policy).collect();
    assert_eq!(policies, [HostInputPolicy::Consume; 3]);
    let mut want = stitched();
    for port in &mut want.host_inputs {
        port.policy = HostInputPolicy::Consume;
    }
    assert_eq!(back, want);
}

#[test]
fn policies_that_do_not_match_the_inputs_are_refused() {
    let short = |w: &mut Writer| {
        write_stitched_as_first_stored(w, &stitched())?;
        write_input_policies(w, &stitched().host_inputs[..2])
    };
    let mut w = Writer::new();
    w.record(short).unwrap();
    assert!(read(&w.buf).is_err());

    let unknown = |w: &mut Writer| {
        write_stitched_as_first_stored(w, &stitched())?;
        w.len(3)?;
        for tag in [0, 1, 9] {
            w.u8(tag);
        }
        Ok(())
    };
    let mut w = Writer::new();
    w.record(unknown).unwrap();
    assert!(matches!(
        read(&w.buf),
        Err(WireError::BadTag {
            what: "host input policy",
            tag: 9
        })
    ));
}
