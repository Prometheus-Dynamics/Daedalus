use daedalus_backend::Backend;
use daedalus_graph::{EdgeDoc, NodeDoc, NodeKindDoc, RateDoc};
use daedalus_planner::assign::ASSIGN_BUDGET;
use daedalus_planner::regions::rules::{Rule, check};
use daedalus_planner::regions::{Input, RegionError, Regions, build};
use daedalus_planner_tests::{Named, decl, edge, node, solved, u8_ty};
use daedalus_registry::Registry;
use daedalus_sim::{BACKEND_ID, SimBackend, SimConfig};

fn registry() -> Registry {
    let mut registry = Registry::new();
    for (id, backends) in [
        ("a", &["cpu"][..]),
        ("b", &["gpu"][..]),
        ("s", &[BACKEND_ID][..]),
    ] {
        let mut d = decl(&format!("t.{id}"))
            .optional("x", u8_ty())
            .optional("y", u8_ty())
            .output("out", u8_ty());
        for backend in backends {
            d = d.body(backend);
        }
        d.add(&mut registry);
    }
    registry
}

fn build_with(
    registry: &Registry,
    nodes: Vec<NodeDoc>,
    edges: Vec<EdgeDoc>,
    backends: &[&dyn Backend],
) -> Result<Regions, RegionError> {
    let (graph, types) = solved(registry, nodes, edges);
    build(&Input {
        graph: &graph,
        registry,
        backends,
        types: &types,
        budget: ASSIGN_BUDGET,
        merge_allowed: true,
    })
}

fn chain(ids: &[&str]) -> (Vec<NodeDoc>, Vec<EdgeDoc>) {
    let nodes = ids.iter().map(|id| node(id)).collect();
    let edges = (1..ids.len() as u32)
        .map(|i| edge(i - 1, "out", i, "x"))
        .collect();
    (nodes, edges)
}

fn regions_of(result: &Regions) -> Vec<Vec<u32>> {
    result
        .regions
        .iter()
        .map(|region| region.formed.nodes.clone())
        .collect()
}

const CPU: Named = Named("cpu");
const GPU: Named = Named("gpu");

#[test]
fn chain_on_one_backend_is_one_region() {
    let (nodes, edges) = chain(&["t.a", "t.a", "t.a"]);
    let result = build_with(&registry(), nodes, edges, &[&CPU]).unwrap();
    assert_eq!(regions_of(&result), [vec![0, 1, 2]]);
}

#[test]
fn backend_change_splits() {
    let (nodes, edges) = chain(&["t.a", "t.a", "t.b", "t.b"]);
    let result = build_with(&registry(), nodes, edges, &[&CPU, &GPU]).unwrap();
    assert_eq!(regions_of(&result), [vec![0, 1], vec![2, 3]]);
    assert_eq!(result.regions[0].formed.outputs.len(), 1);
    assert_eq!(result.regions[1].formed.inputs.len(), 1);
}

fn diamond() -> (Vec<NodeDoc>, Vec<EdgeDoc>) {
    (
        vec![node("t.a"), node("t.b"), node("t.a")],
        vec![
            edge(0, "out", 1, "x"),
            edge(0, "out", 2, "x"),
            edge(1, "out", 2, "y"),
        ],
    )
}

#[test]
fn nonconvex_merge_is_refused() {
    let (nodes, edges) = diamond();
    let result = build_with(&registry(), nodes, edges, &[&CPU, &GPU]).unwrap();
    assert_eq!(regions_of(&result), [vec![0], vec![1], vec![2]]);
}

#[test]
fn disconnected_set_fails_r1() {
    let (nodes, edges) = chain(&["t.a", "t.a", "t.a"]);
    let (graph, _) = solved(&registry(), nodes, edges);
    assert_eq!(check(&graph, &[0, 2]), Err(Rule::R1Connected));
}

#[test]
fn path_out_and_back_fails_r2() {
    let (nodes, edges) = diamond();
    let (graph, _) = solved(&registry(), nodes, edges);
    assert_eq!(check(&graph, &[0, 2]), Err(Rule::R2Convex));
}

#[test]
fn mixed_rate_fails_r3() {
    let mut half = node("t.a");
    half.rate = Some(RateDoc { num: 1, den: 2 });
    let (graph, _) = solved(
        &registry(),
        vec![half, node("t.a")],
        vec![edge(0, "out", 1, "x")],
    );
    assert_eq!(check(&graph, &[0, 1]), Err(Rule::R3OneRate));
}

fn sim(config: SimConfig) -> Result<Regions, RegionError> {
    let mut host = node("host");
    host.kind = NodeKindDoc::HostBridge;
    host.port_types.insert("in".to_string(), "u8".to_string());
    let backend = SimBackend::new(config);
    build_with(
        &registry(),
        vec![node("t.s"), host],
        vec![edge(0, "out", 1, "in")],
        &[&backend],
    )
}

#[test]
fn a_dropped_boundary_output_fails_r4() {
    let error = sim(SimConfig {
        drop_output: true,
        ..SimConfig::default()
    })
    .unwrap_err();
    assert!(
        matches!(error, RegionError::BackendContract { rule: "R4", .. }),
        "{error:?}"
    );
}

#[test]
fn omitted_covers_fail_r5() {
    let error = sim(SimConfig {
        omit_covers: true,
        ..SimConfig::default()
    })
    .unwrap_err();
    assert!(
        matches!(error, RegionError::BackendContract { rule: "R5", .. }),
        "{error:?}"
    );
}

#[test]
fn an_honest_backend_passes_the_contract() {
    let result = sim(SimConfig::default()).unwrap();
    assert_eq!(result.rounds, 1);
}
