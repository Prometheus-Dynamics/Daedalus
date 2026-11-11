//! The counted budgets, measured on this lane's device.

use daedalus_abi::counting::CountingAlloc;
use daedalus_gpu_budgets::{
    ALLOCATIONS_PER_KERNEL_BATCH, ALLOCATIONS_PER_STEP, DISPATCHES_PER_MERGED_CHAIN,
    DISPATCHES_PER_SHADER_NODE, DOWNLOADS_PER_BOUNDARY, Miss, PIPELINES_PER_MERGED_CHAIN,
    SUBMITS_PER_REGION, UPLOADS_PER_BOUNDARY, boundaries, exactly, kernel_batch, merged_chain,
    region, shader_line,
};
use daedalus_wgpu_conformance::{Counted, private};

#[global_allocator]
static COUNTING: CountingAlloc = CountingAlloc;

#[test]
fn a_region_is_one_submit_and_a_pass_for_each_node() {
    let Some(backend) = private() else { return };
    for nodes in [1u64, 2, 8] {
        let counted = shader_line(&backend, nodes as usize, 4096);
        let runs = nodes * DISPATCHES_PER_SHADER_NODE;
        assert_eq!(region(&counted, runs), Ok(()), "{nodes} nodes: {counted:?}");
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
            region(&warm, DISPATCHES_PER_MERGED_CHAIN),
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
    // One shader or eight, a warm step allocates the same: nothing.
    for nodes in [1, 8] {
        let measured = shader_line(&backend, nodes, 4096).allocations;
        assert_eq!(measured, ALLOCATIONS_PER_STEP, "{nodes} shaders");
    }
}

#[test]
fn a_kernel_batch_allocates_nothing_once_its_ops_have_run() {
    let Some(backend) = private() else { return };
    for ops in [1, 8, 40] {
        let measured = kernel_batch(&backend, ops);
        assert_eq!(measured, ALLOCATIONS_PER_KERNEL_BATCH, "{ops} ops");
    }
}

#[test]
fn a_region_that_submits_twice_fails_its_budget() {
    // Planted: what a region that split its work over two submits would measure.
    let planted = Counted {
        submits: 2,
        passes: 1,
        dispatches: 3,
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
            dispatches: first.dispatches + second.dispatches,
            ..Counted::default()
        }
    };
    assert!(region(&twice, 2).is_err_and(|miss| miss.what == "submits per region"));
    // A second pass, a shader run too many, a pipeline built while running and a transfer are
    // each refused too.
    let fine = Counted {
        submits: 1,
        passes: 1,
        dispatches: 2,
        ..Counted::default()
    };
    assert_eq!(region(&fine, 2), Ok(()));
    for (what, wrong) in [
        ("passes per region", Counted { passes: 2, ..fine }),
        (
            "shader runs per region",
            Counted {
                dispatches: 3,
                ..fine
            },
        ),
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
