use daedalus_types::*;

#[test]
fn scalar_rust_names() {
    let table = [
        (Scalar::Unit, "()"),
        (Scalar::Bool, "bool"),
        (Scalar::I8, "i8"),
        (Scalar::I16, "i16"),
        (Scalar::I32, "i32"),
        (Scalar::Int, "i64"),
        (Scalar::ISize, "isize"),
        (Scalar::U8, "u8"),
        (Scalar::U16, "u16"),
        (Scalar::U32, "u32"),
        (Scalar::U64, "u64"),
        (Scalar::USize, "usize"),
        (Scalar::F32, "f32"),
        (Scalar::Float, "f64"),
        (Scalar::String, "String"),
        (Scalar::Bytes, "Vec<u8>"),
    ];
    for (scalar, name) in table {
        assert_eq!(scalar.rust_name(), name);
    }
}

#[test]
fn typekey_accepts_charset_and_rejects_uppercase() {
    const GOOD: TypeKey = TypeKey::new("acme.image-v2:rgba_8");
    assert_eq!(GOOD.as_str(), "acme.image-v2:rgba_8");
    assert_eq!(TypeKey::shared("acme.image-v2:rgba_8").unwrap(), GOOD);
    assert_eq!(TypeKey::shared("Acme").unwrap_err().code, Code::InvalidKey);
    assert_eq!(TypeKey::shared("").unwrap_err().code, Code::InvalidKey);
    assert_eq!(
        TypeKey::shared("{\"a\":1}").unwrap_err().code,
        Code::InvalidKey
    );
}

const fn field(name: &'static str, ty: TypeExpr) -> Field {
    Field { name, ty }
}

static INT: TypeExpr = TypeExpr::Scalar(Scalar::Int);
static LIST: TypeExpr = TypeExpr::List(&INT);
static POINT: [Field; 2] = [
    field("x", TypeExpr::Scalar(Scalar::F32)),
    field("y", TypeExpr::Scalar(Scalar::F32)),
];
static SWAPPED: [Field; 2] = [
    field("y", TypeExpr::Scalar(Scalar::F32)),
    field("x", TypeExpr::Scalar(Scalar::F32)),
];

#[test]
fn typeexpr_structural_hash_vectors() {
    assert_eq!(
        format!("{:016x}", INT.structural_hash()),
        "08328307b4eb676e"
    );
    assert_eq!(
        format!("{:016x}", LIST.structural_hash()),
        "e1f68a1870f72949"
    );
    assert_eq!(
        format!("{:016x}", TypeExpr::Struct(&POINT).structural_hash()),
        "4137802d75b5b13a"
    );
    assert_ne!(
        TypeExpr::Struct(&POINT).structural_hash(),
        TypeExpr::Struct(&SWAPPED).structural_hash()
    );
    assert_eq!(
        TypeExpr::List(&INT).structural_hash(),
        LIST.structural_hash()
    );
}

#[test]
fn stable_id128_pinned() {
    assert_eq!(
        stable_id128("node", "demo.add"),
        0x7a7d_d50d_46b1_67a4_f2d5_f544_da9f_1993
    );
    assert_eq!(
        stable_id128("node", "io.host_bridge"),
        0x4c63_02d1_8f37_873e_6b9a_c261_8d54_9425
    );
    assert_eq!(
        stable_id128("plugin", "daedalus.builtin.primitive_types"),
        0xd68a_1efc_ba14_c374_76ff_8dfe_fbe3_1803
    );
    assert_eq!(fnv1a64(b"foobar"), 0x8594_4171_f739_67e8);
}

#[test]
fn literal_range_checks() {
    let err = u8::from_literal(&Literal::Int(300)).unwrap_err();
    assert_eq!(err.code, Code::OutOfRange);
    assert!(err.detail.contains("u8"));
    assert_eq!(u8::from_literal(&Literal::Int(200)), Ok(200));
    assert_eq!(f64::from_literal(&Literal::Int(2)), Ok(2.0));
    assert_eq!(
        f64::from_literal(&Literal::Int(i64::MAX - 1))
            .unwrap_err()
            .code,
        Code::OutOfRange
    );
    assert_eq!(
        bool::from_literal(&Literal::Int(1)).unwrap_err().code,
        Code::InvalidLiteral
    );
    assert_eq!(<&str>::from_literal(&Literal::Str("a")), Ok("a"));
}

#[test]
fn typed_reads_in_const_context() {
    const KEY: TypeKey = <u32 as Typed>::KEY;
    const EXPR: TypeExpr = <String as Typed>::EXPR;
    assert_eq!(KEY.as_str(), "u32");
    assert_eq!(EXPR, TypeExpr::Scalar(Scalar::String));
}

#[test]
fn bad_literals_do_not_compile() {
    trybuild::TestCases::new().compile_fail("tests/ui/*.rs");
}
