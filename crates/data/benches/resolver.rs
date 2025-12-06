use criterion::{Criterion, criterion_group, criterion_main};
use daedalus_data::convert::{ConverterBuilder, ConverterGraph};
use daedalus_data::model::{TypeExpr, ValueType};

fn build_chain(len: usize) -> ConverterGraph {
    let mut graph = ConverterGraph::new();
    let types: Vec<TypeExpr> = (0..=len)
        .map(|i| TypeExpr::Tuple(vec![TypeExpr::Scalar(ValueType::Int); i]))
        .collect();
    for i in 0..len {
        graph.register(
            ConverterBuilder::new(format!("c{i}"), types[i].clone(), types[i + 1].clone(), Ok)
                .cost(1)
                .build_boxed(),
        );
    }
    graph
}

fn resolve_bench(c: &mut Criterion) {
    let graph = build_chain(10);
    let from = TypeExpr::Tuple(Vec::new());
    let to = TypeExpr::Tuple(vec![TypeExpr::Scalar(ValueType::Int); 10]);
    c.bench_function("resolver_chain_len_10", |b| {
        b.iter(|| {
            let _ = graph.resolve(&from, &to).unwrap();
        })
    });
}

criterion_group!(benches, resolve_bench);
criterion_main!(benches);
