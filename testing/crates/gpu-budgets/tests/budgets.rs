//! The counted budgets, measured on this lane's device.

use daedalus_abi::counting::CountingAlloc;
use daedalus_gpu_budgets::{
    ALLOCATION_SLACK, ALLOCATIONS_PER_PASS, ALLOCATIONS_PER_STEP, DOWNLOADS_PER_BOUNDARY, Miss,
    PASSES_PER_MERGED_CHAIN, PASSES_PER_SHADER_NODE, PIPELINES_PER_MERGED_CHAIN,
    SUBMITS_PER_REGION, UPLOADS_PER_BOUNDARY, boundaries, exactly, merged_chain, region,
    shader_line,
};
use daedalus_wgpu_conformance::{Counted, private};

#[global_allocator]
static COUNTING: CountingAlloc = CountingAlloc;

#[test]
fn a_region_is_one_submit_and_a_pass_for_each_node() {
    let Some(backend) = private() else { return };
    for nodes in [1u64, 2, 8] {
        let counted = shader_line(&backend, nodes as usize, 4096);
        let passes = nodes * PASSES_PER_SHADER_NODE;
        assert_eq!(
            region(&counted, passes),
            Ok(()),
            "{nodes} nodes: {counted:?}"
        );
    }
}

#[test]
fn a_merged_chain_is_one_pipeline_and_one_pass() {
    let Some(backend) = private() else { return };
    for pairs in [1, 3] {
        let (built, warm) = merged_chain(&backend, pairs, 4096);
        assert_eq!(
            exactly(
                "pipelines per merged chain",
                built.pipelines,
                PIPELINES_PER_MERGED_CHAIN
            ),
            Ok(()),
            "{pairs} pairs"
        );
        assert_eq!(
            region(&warm, PASSES_PER_MERGED_CHAIN),
            Ok(()),
            "{pairs} pairs: {warm:?}"
        );
    }
}

#[test]
fn a_value_crosses_each_boundary_once() {
    let Some(backend) = private() else { return };
    let counted = boundaries(&backend, 64 * 1024);
    assert_eq!(
        exactly(
            "uploads per boundary",
            counted.uploads,
            UPLOADS_PER_BOUNDARY
        ),
        Ok(())
    );
    assert_eq!(
        exactly(
            "downloads per boundary",
            counted.downloads,
            DOWNLOADS_PER_BOUNDARY
        ),
        Ok(())
    );
    assert_eq!(counted.passes, 0, "a copy is no shader");
}

#[test]
fn the_allocation_budget_is_what_is_measured() {
    let Some(backend) = private() else { return };
    // The budget is a ceiling in `region`. Here it is checked not to have gone slack: a step
    // that allocates less has earned a lower constant.
    let (one, eight) = (
        shader_line(&backend, 1, 4096).allocations,
        shader_line(&backend, 8, 4096).allocations,
    );
    let per_pass = (eight - one).div_ceil(7);
    let base = one - per_pass;
    assert_eq!(
        per_pass, ALLOCATIONS_PER_PASS,
        "one pass {one}, eight {eight}"
    );
    assert!(
        base <= ALLOCATIONS_PER_STEP && base + ALLOCATION_SLACK >= ALLOCATIONS_PER_STEP,
        "a step with one pass allocates {one}: {base} beside its pass, where the budget is {ALLOCATIONS_PER_STEP}"
    );
}

#[test]
fn a_region_that_submits_twice_fails_its_budget() {
    // Planted: what a region that split its work over two submits would measure.
    let planted = Counted {
        submits: 2,
        passes: 3,
        ..Counted::default()
    };
    assert_eq!(
        region(&planted, 3),
        Err(Miss {
            what: "submits per region",
            measured: 2,
            budget: SUBMITS_PER_REGION,
        })
    );
    // And measured for real: two runs of a step are two submits, which the budget for one
    // region refuses.
    let Some(backend) = private() else { return };
    let twice = {
        let (first, second) = (shader_line(&backend, 1, 64), shader_line(&backend, 1, 64));
        Counted {
            submits: first.submits + second.submits,
            passes: first.passes + second.passes,
            ..Counted::default()
        }
    };
    assert!(region(&twice, 2).is_err_and(|miss| miss.what == "submits per region"));
    // An extra pass, a pipeline built while running and a transfer are each refused too.
    let fine = Counted {
        submits: 1,
        passes: 2,
        ..Counted::default()
    };
    assert_eq!(region(&fine, 2), Ok(()));
    for (what, wrong) in [
        ("passes per region", Counted { passes: 3, ..fine }),
        (
            "pipelines built by a run",
            Counted {
                pipelines: 1,
                ..fine
            },
        ),
        ("transfers by a region", Counted { uploads: 1, ..fine }),
        (
            "allocations per step",
            Counted {
                allocations: 10_000,
                ..fine
            },
        ),
    ] {
        assert!(
            region(&wrong, 2).is_err_and(|miss| miss.what == what),
            "{what}"
        );
    }
}
