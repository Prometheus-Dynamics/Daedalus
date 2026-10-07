//! One camera's preprocessing (mask prep + quads) shared by two detector graphs in an execution
//! domain: it runs once per frame, the detectors' outputs equal two separate full detectors',
//! and the frame path copies and allocates nothing, hand-laid and found by structural sharing.

use std::sync::Mutex;

use daedalus::engine::{EngineConfig, ExecutionDomain, MetricsLevel};
use daedalus::runtime::handler_registry::HandlerRegistry;
use daedalus_frame_bench::{
    CountingAllocator, DETECTOR_OUTPUTS, Detections, Dictionary, FrameBenchConfig, FrameFeed,
    FrameSourceConfig, PREPROCESS_GRAPH, RefinedCorners, RejectedMarkers, SyntheticFrameSource,
    compile_separate_detectors, compile_shared_detectors, compile_structural_detectors,
    dictionary_name, run_domain_bench,
};

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator::system();

/// Allocation counters are process-wide: one measurement at a time.
static SERIAL: Mutex<()> = Mutex::new(());

const DICTIONARIES: [Dictionary; 2] = [Dictionary::Aruco4x4_50, Dictionary::AprilTag36h11];

fn config() -> EngineConfig {
    EngineConfig::default().with_metrics_level(MetricsLevel::Off)
}

fn source() -> SyntheticFrameSource {
    SyntheticFrameSource::new(FrameSourceConfig {
        width: 64,
        height: 48,
        feed: FrameFeed::Owner,
        ..FrameSourceConfig::default()
    })
    .expect("frame source")
}

/// Every detector's three outputs of one frame.
type Outputs = Vec<(Detections, RejectedMarkers, RefinedCorners)>;

fn frame(
    domain: &mut ExecutionDomain<HandlerRegistry>,
    source: &mut SyntheticFrameSource,
) -> Outputs {
    domain
        .push_payload("frame", source.next_payload())
        .expect("push");
    let tick = domain.tick();
    assert!(tick.is_ok(), "{tick:?}");
    DICTIONARIES
        .iter()
        .map(|&dictionary| {
            let name = dictionary_name(dictionary);
            let mut take = |port: &str| domain.take_payload(name, port).expect(port);
            let detections = take("detections");
            let rejected = take("rejected");
            let refined = take("refined_corners");
            (
                detections
                    .get_ref::<Detections>()
                    .expect("detections")
                    .clone(),
                rejected
                    .get_ref::<RejectedMarkers>()
                    .expect("rejected")
                    .clone(),
                refined
                    .get_ref::<RefinedCorners>()
                    .expect("refined")
                    .clone(),
            )
        })
        .collect()
}

#[test]
fn shared_preprocessing_matches_separate_detectors() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut separate = compile_separate_detectors(&DICTIONARIES, config()).expect("separate");
    let mut shared = compile_shared_detectors(&DICTIONARIES, config()).expect("shared");
    let mut structural = compile_structural_detectors(&DICTIONARIES, config()).expect("loaded");
    let (mut a, mut b, mut c) = (source(), source(), source());
    for _ in 0..32 {
        let expected = frame(&mut separate, &mut a);
        assert!(!expected[0].0.items.is_empty());
        assert_eq!(frame(&mut shared, &mut b), expected);
        assert_eq!(frame(&mut structural, &mut c), expected);
    }
    // Preprocessing ran once per frame for both detectors.
    let stats = shared.stats();
    let preprocess = stats.graph(PREPROCESS_GRAPH).expect("preprocess");
    assert_eq!((preprocess.runs, preprocess.consumers), (32, 2));
    assert_eq!(
        (preprocess.avoided_runs, preprocess.avoided_node_runs),
        (32, 64)
    );
    let loaded = structural.stats();
    assert_eq!(loaded.graph("shared").expect("upstream").runs, 32);
    assert_eq!(loaded.avoided_node_runs, 64);

    let explanation = structural.explain();
    let ids: Vec<&str> = explanation
        .shared_nodes
        .iter()
        .map(|node| node.node_id.as_str())
        .collect();
    assert_eq!(
        ids,
        [
            "daedalus.frame_bench.detector:mask_prep_runs",
            "daedalus.frame_bench.detector:quads_from_runs"
        ],
        "{explanation}"
    );
    let detector = explanation
        .graph(dictionary_name(DICTIONARIES[0]))
        .expect("detector");
    assert_eq!(
        detector.nodes,
        ["decode", "validate", "refine"],
        "{explanation}"
    );
    assert!(
        explanation.links.iter().all(|link| link.zero_copy()),
        "{explanation}"
    );
}

#[test]
fn shared_preprocessing_frames_allocate_and_copy_nothing() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for structural in [false, true] {
        let mut domain = if structural {
            compile_structural_detectors(&DICTIONARIES, config())
        } else {
            compile_shared_detectors(&DICTIONARIES, config())
        }
        .expect("domain");
        domain.enable_frame_overhead(256);
        let outputs: Vec<(&str, &str)> = DICTIONARIES
            .iter()
            .flat_map(|&d| DETECTOR_OUTPUTS.map(|port| (dictionary_name(d), port)))
            .collect();
        let run = run_domain_bench(
            &mut domain,
            &mut source(),
            &FrameBenchConfig::new(DETECTOR_OUTPUTS).with_ticks(64, 256),
            &outputs,
        )
        .expect("domain bench");
        let context = format!("structural {structural}\n{run}");
        let overhead = run.overhead.as_ref().expect("overhead");
        assert_eq!(overhead.graphs.len(), 3, "{context}");
        for (graph, report) in &overhead.graphs {
            assert!(report.alloc_probe, "the counting allocator is installed");
            for counter in ["copies", "runtime_allocs", "node_allocs", "host_allocs"] {
                let max = report.counter(counter).expect(counter).max;
                assert_eq!(max, 0, "{graph} {counter}: {context}");
            }
        }
        let [runtime, node, host, _other] = run.allocs_per_frame.expect("alloc probe");
        assert_eq!((runtime, node, host), (0.0, 0.0, 0.0), "{context}");
        assert_eq!(overhead.stats.avoided_runs, 256, "{context}");
    }
}
