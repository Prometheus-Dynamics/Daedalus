use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use daedalus_data::convert::{ConverterBuilder, ConverterGraph};
use daedalus_data::model::{TypeExpr, Value};

fn opaque(id: impl Into<String>) -> TypeExpr {
    TypeExpr::Opaque(id.into())
}

fn build_chain(len: usize) -> ConverterGraph {
    let mut graph = ConverterGraph::new();
    let types: Vec<TypeExpr> = (0..=len).map(|i| opaque(format!("chain.{i}"))).collect();
    for i in 0..len {
        graph.register(
            ConverterBuilder::new(format!("c{i}"), types[i].clone(), types[i + 1].clone(), Ok)
                .cost(1)
                .build_boxed(),
        );
    }
    graph
}

fn build_branching_graph(stages: usize, branches_per_stage: usize) -> ConverterGraph {
    let mut graph = ConverterGraph::new();
    for stage in 0..stages {
        let from = opaque(format!("branch.{stage}.main"));
        let to = opaque(format!("branch.{}.main", stage + 1));
        graph.register(
            ConverterBuilder::new(
                format!("branch.{stage}.main"),
                from.clone(),
                to.clone(),
                Ok::<Value, _>,
            )
            .cost(1)
            .build_boxed(),
        );

        for branch in 0..branches_per_stage {
            let gated = opaque(format!("branch.{stage}.gated.{branch}"));
            graph.register(
                ConverterBuilder::new(
                    format!("branch.{stage}.feature.{branch}"),
                    from.clone(),
                    gated.clone(),
                    Ok::<Value, _>,
                )
                .cost(1)
                .feature_flag("release-bench-feature")
                .build_boxed(),
            );
            graph.register(
                ConverterBuilder::new(
                    format!("branch.{stage}.gpu.{branch}"),
                    from.clone(),
                    gated,
                    Ok::<Value, _>,
                )
                .cost(1)
                .requires_gpu(true)
                .build_boxed(),
            );
        }
    }
    graph
}

fn resolve_bench(c: &mut Criterion) {
    let chain_10 = build_chain(10);
    let chain_10_from = opaque("chain.0");
    let chain_10_to = opaque("chain.10");
    c.bench_function("resolver_chain_len_10", |b| {
        b.iter(|| {
            black_box(
                chain_10
                    .resolve(black_box(&chain_10_from), black_box(&chain_10_to))
                    .unwrap(),
            );
        })
    });

    let chain_100 = build_chain(100);
    let chain_100_from = opaque("chain.0");
    let chain_100_to = opaque("chain.100");
    c.bench_function("resolver_chain_len_100", |b| {
        b.iter(|| {
            black_box(
                chain_100
                    .resolve(black_box(&chain_100_from), black_box(&chain_100_to))
                    .unwrap(),
            );
        })
    });

    let branching = build_branching_graph(40, 3);
    let branch_from = opaque("branch.0.main");
    let branch_to = opaque("branch.40.main");
    c.bench_function("resolver_branching_gated_40x3", |b| {
        b.iter(|| {
            black_box(
                branching
                    .resolve_with_context(
                        black_box(&branch_from),
                        black_box(&branch_to),
                        black_box(&[]),
                        black_box(false),
                    )
                    .unwrap(),
            );
        })
    });
}

criterion_group!(benches, resolve_bench);
criterion_main!(benches);
