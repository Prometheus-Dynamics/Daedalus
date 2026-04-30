use super::*;
use crate::model::{TypeExpr, Value, ValueType};
use once_cell::sync::Lazy;
use proptest::prelude::*;
#[cfg(feature = "async")]
use std::future::Future;
#[cfg(feature = "async")]
use std::pin::Pin;
#[cfg(feature = "async")]
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

struct Identity {
    id: ConverterId,
    ty: TypeExpr,
}

impl Converter for Identity {
    fn id(&self) -> ConverterId {
        self.id.clone()
    }
    fn input(&self) -> &TypeExpr {
        &self.ty
    }
    fn output(&self) -> &TypeExpr {
        &self.ty
    }
    fn cost(&self) -> u64 {
        0
    }
    fn convert(&self, v: Value) -> DataResult<Value> {
        Ok(v)
    }
}

struct BoolToInt;
impl Converter for BoolToInt {
    fn id(&self) -> ConverterId {
        ConverterId("bool_to_int".into())
    }
    fn input(&self) -> &TypeExpr {
        static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::Bool));
        &TY
    }
    fn output(&self) -> &TypeExpr {
        static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::Int));
        &TY
    }
    fn cost(&self) -> u64 {
        1
    }
    fn convert(&self, v: Value) -> DataResult<Value> {
        match v {
            Value::Bool(b) => Ok(Value::Int(if b { 1 } else { 0 })),
            _ => Err(DataError::new(DataErrorCode::InvalidType, "expected bool")),
        }
    }
}

struct IntToString;
impl Converter for IntToString {
    fn id(&self) -> ConverterId {
        ConverterId("int_to_string".into())
    }
    fn input(&self) -> &TypeExpr {
        static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::Int));
        &TY
    }
    fn output(&self) -> &TypeExpr {
        static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::String));
        &TY
    }
    fn cost(&self) -> u64 {
        2
    }
    fn convert(&self, v: Value) -> DataResult<Value> {
        match v {
            Value::Int(i) => Ok(Value::String(i.to_string().into())),
            _ => Err(DataError::new(DataErrorCode::InvalidType, "expected int")),
        }
    }
}

struct GpuOnly;
impl Converter for GpuOnly {
    fn id(&self) -> ConverterId {
        ConverterId("gpu_only".into())
    }
    fn input(&self) -> &TypeExpr {
        static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::Int));
        &TY
    }
    fn output(&self) -> &TypeExpr {
        static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::Float));
        &TY
    }
    fn cost(&self) -> u64 {
        1
    }
    fn requires_gpu(&self) -> bool {
        true
    }
    fn convert(&self, v: Value) -> DataResult<Value> {
        match v {
            Value::Int(i) => Ok(Value::Float(i as f64)),
            _ => Err(DataError::new(DataErrorCode::InvalidType, "expected int")),
        }
    }
}

#[test]
fn resolves_trivial_path() {
    let mut graph = ConverterGraph::new();
    let ty = TypeExpr::Scalar(ValueType::Int);
    graph.register(Box::new(Identity {
        id: ConverterId("id".into()),
        ty: ty.clone(),
    }));
    let res = graph.resolve(&ty, &ty).expect("resolve");
    assert_eq!(res.provenance.total_cost, 0u64);
    assert!(res.provenance.steps.is_empty());
}

#[test]
fn resolves_multi_step_path() {
    let mut graph = ConverterGraph::new();
    graph.register(Box::new(BoolToInt));
    graph.register(Box::new(IntToString));
    let from = TypeExpr::Scalar(ValueType::Bool);
    let to = TypeExpr::Scalar(ValueType::String);
    let res = graph.resolve(&from, &to).expect("resolve");
    assert_eq!(res.provenance.total_cost, 3u64);
    assert_eq!(
        res.provenance.steps,
        vec![
            ConverterId("bool_to_int".into()),
            ConverterId("int_to_string".into())
        ]
    );
}

#[test]
fn respects_gpu_flag() {
    let mut graph = ConverterGraph::new();
    graph.register(Box::new(GpuOnly));
    let from = TypeExpr::Scalar(ValueType::Int);
    let to = TypeExpr::Scalar(ValueType::Float);
    let err = graph
        .resolve_with_context(&from, &to, &[], false)
        .unwrap_err();
    assert_eq!(err.code(), DataErrorCode::UnknownConverter);
    assert!(err.message().contains("gpu_only"));
    let res = graph
        .resolve_with_context(&from, &to, &[], true)
        .expect("resolve");
    assert_eq!(res.provenance.steps, vec![ConverterId("gpu_only".into())]);
}

#[test]
fn cycles_do_not_hang() {
    struct AtoB;
    impl Converter for AtoB {
        fn id(&self) -> ConverterId {
            ConverterId("a_to_b".into())
        }
        fn input(&self) -> &TypeExpr {
            static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::Bool));
            &TY
        }
        fn output(&self) -> &TypeExpr {
            static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::Int));
            &TY
        }
        fn cost(&self) -> u64 {
            1
        }
        fn convert(&self, v: Value) -> DataResult<Value> {
            Ok(v)
        }
    }

    struct BtoA;
    impl Converter for BtoA {
        fn id(&self) -> ConverterId {
            ConverterId("b_to_a".into())
        }
        fn input(&self) -> &TypeExpr {
            static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::Int));
            &TY
        }
        fn output(&self) -> &TypeExpr {
            static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::Bool));
            &TY
        }
        fn cost(&self) -> u64 {
            1
        }
        fn convert(&self, v: Value) -> DataResult<Value> {
            Ok(v)
        }
    }

    let mut graph = ConverterGraph::new();
    graph.register(Box::new(AtoB));
    graph.register(Box::new(BtoA));
    let from = TypeExpr::Scalar(ValueType::Bool);
    let to = TypeExpr::Scalar(ValueType::String);
    let err = graph.resolve(&from, &to).unwrap_err();
    assert!(matches!(
        err.code(),
        DataErrorCode::UnknownConverter | DataErrorCode::CycleDetected
    ));
}

#[test]
fn skips_cycles_and_finds_alternate_path() {
    struct AtoB;
    impl Converter for AtoB {
        fn id(&self) -> ConverterId {
            ConverterId("a_to_b".into())
        }
        fn input(&self) -> &TypeExpr {
            static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::Bool));
            &TY
        }
        fn output(&self) -> &TypeExpr {
            static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::Int));
            &TY
        }
        fn cost(&self) -> u64 {
            1
        }
        fn convert(&self, v: Value) -> DataResult<Value> {
            Ok(v)
        }
    }

    struct BtoA;
    impl Converter for BtoA {
        fn id(&self) -> ConverterId {
            ConverterId("b_to_a".into())
        }
        fn input(&self) -> &TypeExpr {
            static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::Int));
            &TY
        }
        fn output(&self) -> &TypeExpr {
            static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::Bool));
            &TY
        }
        fn cost(&self) -> u64 {
            1
        }
        fn convert(&self, v: Value) -> DataResult<Value> {
            Ok(v)
        }
    }

    struct BtoString;
    impl Converter for BtoString {
        fn id(&self) -> ConverterId {
            ConverterId("b_to_string".into())
        }
        fn input(&self) -> &TypeExpr {
            static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::Int));
            &TY
        }
        fn output(&self) -> &TypeExpr {
            static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::String));
            &TY
        }
        fn cost(&self) -> u64 {
            5
        }
        fn convert(&self, v: Value) -> DataResult<Value> {
            Ok(v)
        }
    }

    let mut graph = ConverterGraph::new();
    graph.register(Box::new(AtoB));
    graph.register(Box::new(BtoA));
    graph.register(Box::new(BtoString));
    let from = TypeExpr::Scalar(ValueType::Bool);
    let to = TypeExpr::Scalar(ValueType::String);
    let res = graph.resolve(&from, &to).expect("resolve");
    assert_eq!(
        res.provenance.steps,
        vec![
            ConverterId("a_to_b".into()),
            ConverterId("b_to_string".into())
        ]
    );
    assert_eq!(
        res.provenance.skipped_cycles,
        vec![ConverterId("b_to_a".into())]
    );
}

#[test]
fn errors_when_no_path() {
    let graph = ConverterGraph::new();
    let from = TypeExpr::Scalar(ValueType::Bool);
    let to = TypeExpr::Scalar(ValueType::String);
    let err = graph.resolve(&from, &to).unwrap_err();
    assert_eq!(err.code(), DataErrorCode::UnknownConverter);
}

#[test]
fn builder_sorts_feature_flags() {
    let conv = ConverterBuilder::new(
        "id",
        TypeExpr::Scalar(ValueType::Int),
        TypeExpr::Scalar(ValueType::String),
        Ok,
    )
    .feature_flag("b")
    .feature_flag("a")
    .build();
    assert_eq!(conv.feature_flags, vec!["a", "b"]);
}

proptest! {
    #[test]
    fn chain_costs_are_additive(len in 1usize..6) {
        let mut graph = ConverterGraph::new();
        let types: Vec<TypeExpr> = (0..=len).map(|i| {
            let v = vec![TypeExpr::Scalar(ValueType::Int); i];
            TypeExpr::Tuple(v)
        }).collect();
        for i in 0..len {
            let input = types[i].clone();
            let output = types[i + 1].clone();
            graph.register(ConverterBuilder::new(
                format!("c{i}"),
                input.clone(),
                output.clone(),
                Ok,
            ).cost(1).build_boxed());
        }
        let res = graph.resolve(&types[0], &types[len]).expect("resolve chain");
        prop_assert_eq!(res.provenance.steps.len(), len);
        prop_assert_eq!(res.provenance.total_cost, len as u64);
    }

    #[test]
    fn feature_flag_filtering(allows in proptest::bool::ANY) {
        let mut graph = ConverterGraph::new();
        graph.register(
            ConverterBuilder::new(
                "flagged",
                TypeExpr::Scalar(ValueType::Int),
                TypeExpr::Scalar(ValueType::Float),
                Ok,
            )
            .feature_flag("feat")
            .build_boxed(),
        );
        let from = TypeExpr::Scalar(ValueType::Int);
        let to = TypeExpr::Scalar(ValueType::Float);
        let features = if allows { vec!["feat".to_string()] } else { vec![] };
        let res = graph.resolve_with_context(&from, &to, &features, true);
        prop_assert_eq!(res.is_ok(), allows);
        if !allows {
            let err = res.unwrap_err();
            prop_assert!(err.message().contains("flagged"));
        }
    }
}

#[cfg(feature = "async")]
fn dummy_raw_waker() -> RawWaker {
    fn no_op(_: *const ()) {}
    fn clone(_: *const ()) -> RawWaker {
        dummy_raw_waker()
    }
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, no_op, no_op, no_op);
    RawWaker::new(std::ptr::null(), &VTABLE)
}

#[cfg(feature = "async")]
fn block_on<F: Future>(mut fut: F) -> F::Output {
    let waker: Waker = unsafe { Waker::from_raw(dummy_raw_waker()) };
    let mut cx = Context::from_waker(&waker);
    let mut fut = unsafe { Pin::new_unchecked(&mut fut) };
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(val) => return val,
            Poll::Pending => continue,
        }
    }
}

#[cfg(feature = "async")]
#[test]
fn async_resolve_matches_sync() {
    let mut graph = ConverterGraph::new();
    graph.register(
        ConverterBuilder::new(
            "id",
            TypeExpr::Scalar(ValueType::Int),
            TypeExpr::Scalar(ValueType::Int),
            Ok,
        )
        .build_boxed(),
    );
    let from = TypeExpr::Scalar(ValueType::Int);
    let to = TypeExpr::Scalar(ValueType::Int);
    let sync = graph.resolve(&from, &to).unwrap();
    let async_res = block_on(graph.resolve_async(&from, &to)).unwrap();
    assert_eq!(sync.provenance, async_res.provenance);
}

#[test]
fn golden_resolution_is_stable() {
    let mut graph = ConverterGraph::new();
    graph.register(Box::new(BoolToInt));
    graph.register(Box::new(IntToString));
    let from = TypeExpr::Scalar(ValueType::Bool);
    let to = TypeExpr::Scalar(ValueType::String);
    let res = graph.resolve(&from, &to).expect("resolve");
    let json = serde_json::to_string(&res).expect("serialize");
    assert_eq!(
        json,
        r#"{"provenance":{"steps":["bool_to_int","int_to_string"],"total_cost":3,"skipped_cycles":[],"skipped_gpu":[],"skipped_features":[]}}"#
    );
}
