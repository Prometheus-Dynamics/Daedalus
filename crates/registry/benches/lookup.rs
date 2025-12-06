use criterion::{Criterion, criterion_group, criterion_main};
use daedalus_data::convert::{ConverterBuilder, ConverterId};
use daedalus_data::descriptor::DescriptorBuilder;
use daedalus_data::model::{TypeExpr, ValueType};
use daedalus_registry::store::Registry;

fn build_registry() -> Registry {
    let mut reg = Registry::new();
    reg.register_value(
        DescriptorBuilder::new("a", "1.0")
            .type_expr(TypeExpr::Scalar(ValueType::Int))
            .build()
            .unwrap(),
    )
    .unwrap();
    reg.register_value(
        DescriptorBuilder::new("b", "1.0")
            .type_expr(TypeExpr::Scalar(ValueType::Bool))
            .build()
            .unwrap(),
    )
    .unwrap();
    reg.register_converter(
        ConverterBuilder::new(
            "bool_to_int",
            TypeExpr::Scalar(ValueType::Bool),
            TypeExpr::Scalar(ValueType::Int),
            Ok,
        )
        .cost(1)
        .build_boxed(),
    )
    .unwrap();
    reg
}

fn registry_lookup_bench(c: &mut Criterion) {
    let reg = build_registry();
    let from = TypeExpr::Scalar(ValueType::Bool);
    let to = TypeExpr::Scalar(ValueType::Int);
    c.bench_function("resolve_bool_to_int", |b| {
        b.iter(|| {
            let res = reg.resolve_converter(&from, &to).unwrap();
            assert_eq!(
                res.provenance.steps,
                vec![ConverterId("bool_to_int".into())]
            );
        })
    });
}

criterion_group!(benches, registry_lookup_bench);
criterion_main!(benches);
