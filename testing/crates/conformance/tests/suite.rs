//! The suite against the sim, and against a backend broken three ways.

use std::sync::atomic::{AtomicU32, Ordering};

use daedalus_abi::{OwnedStep, Slots, Step, StepCx, StepEnd};
use daedalus_backend::*;
use daedalus_conformance::{conformance, run_all};
use daedalus_sim::{SimBackend, SimConfig, SimForm, SimMemory};
use daedalus_types::NodeIndex;

conformance!(SimBackend::default());

mod merged {
    use super::*;
    conformance!(SimBackend::new(SimConfig {
        merge: true,
        ..SimConfig::default()
    }));
}

mod faulting {
    use super::*;
    conformance!(SimBackend::new(SimConfig {
        faults: &[1],
        ..SimConfig::default()
    }));
}

mod blob {
    use super::*;
    conformance!(SimBackend::new(SimConfig {
        form: SimForm::Blob,
        ..SimConfig::default()
    }));
}

mod source {
    use super::*;
    conformance!(SimBackend::new(SimConfig {
        form: SimForm::Source,
        ..SimConfig::default()
    }));
}

mod memory_only {
    use super::*;
    conformance!(SimMemory);
}

struct Allocates;

impl OwnedStep for Allocates {
    fn run(&mut self, _: &mut Slots, _: &mut StepCx) -> StepEnd {
        std::hint::black_box(Box::new(0u8));
        StepEnd::Done
    }
}

/// Answers differently on its second `can_take`, names a stranger when it refuses, and allocates
/// when it runs.
#[derive(Default)]
struct Broken {
    calls: AtomicU32,
}

impl Backend for Broken {
    fn id(&self) -> &str {
        "broken"
    }

    fn memory(&self) -> Vec<MemorySpace> {
        vec![]
    }

    fn transfers(&self) -> Vec<Transfer> {
        vec![]
    }

    fn can_take(&self, offer: &Offer<'_>) -> Vec<NodeRef> {
        let keep = if self.calls.fetch_add(1, Ordering::Relaxed) == 0 {
            2
        } else {
            1
        };
        offer.nodes[..keep]
            .iter()
            .map(|n| NodeRef {
                node: n.node,
                body: n.body,
            })
            .collect()
    }

    fn compile(&self, region: &Region<'_>, _: &CompileCx<'_>) -> Result<Compiled, Refusal> {
        if region.nodes.contains(&NodeIndex(2)) {
            return Err(Refusal::new(vec![NodeIndex(99)], "no"));
        }
        let info = UnitInfo {
            covers: region.nodes.to_vec(),
            ..UnitInfo::default()
        };
        Ok(Compiled::Steps(vec![StepUnit {
            step: Step::boxed(Allocates),
            info,
        }]))
    }
}

#[test]
fn a_broken_backend_fails_exactly_what_it_breaks() {
    let results = run_all(Broken::default);
    let failed: Vec<_> = results
        .iter()
        .filter(|r| !r.passed())
        .map(|r| r.check)
        .collect();
    assert_eq!(
        failed,
        [
            "can_take_is_pure",
            "refusal_names_only_region_nodes",
            "steps_do_not_allocate"
        ],
        "{results:#?}"
    );
    let shown = results.iter().find(|r| !r.passed()).unwrap().to_string();
    assert!(
        shown.contains("broken"),
        "a failure names the backend: {shown}"
    );
    assert!(shown.contains("[0, 1]"), "and the region: {shown}");
}

#[test]
fn a_panicking_backend_fails_instead_of_stopping_the_suite() {
    struct Panics;
    impl Backend for Panics {
        fn id(&self) -> &str {
            "panics"
        }
        fn memory(&self) -> Vec<MemorySpace> {
            panic!("no memory")
        }
        fn transfers(&self) -> Vec<Transfer> {
            vec![]
        }
        fn can_take(&self, _: &Offer<'_>) -> Vec<NodeRef> {
            vec![]
        }
        fn compile(&self, _: &Region<'_>, _: &CompileCx<'_>) -> Result<Compiled, Refusal> {
            Err(Refusal::whole_region("no"))
        }
    }
    let results = run_all(|| Panics);
    let memory = results
        .iter()
        .find(|r| r.check == "memory_ids_are_unique")
        .unwrap();
    assert_eq!(memory.failure.as_deref(), Some("the check panicked"));
}
