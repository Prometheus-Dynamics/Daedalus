#![allow(dead_code)]

use daedalus_author_fixture::sample::Sample;
use daedalus_derive::{NodeConfig, Type};
use daedalus_registry::{FieldRule, TypeRef};
use daedalus_types::{Code, Field, Literal, Scalar, TypeExpr, TypeKey, Typed, Variant};

fn positive(config: &Blur) -> Result<(), &'static str> {
    if config.radius > 0 {
        Ok(())
    } else {
        Err("radius must be positive")
    }
}

#[derive(Clone, Default, NodeConfig)]
#[validate(fn = positive)]
struct Blur {
    #[port(
        default = 3,
        min = 1,
        max = 99,
        odd,
        label = "Radius",
        unit = "px",
        group = "g"
    )]
    radius: u32,
    #[port(name = "strength", default = 0.5, advanced, description = "How much")]
    amount: f64,
    plain: bool,
}

fn lookup<'a>(rows: &'a [(&str, Literal)]) -> impl Fn(&str) -> Option<&'a Literal> {
    move |name| {
        rows.iter()
            .find(|(key, _)| *key == name)
            .map(|(_, value)| value)
    }
}

#[test]
fn fields_carry_the_attributes() {
    let radius = &Blur::FIELDS[0];
    assert_eq!(radius.name, "radius");
    assert_eq!(radius.ty, TypeRef::Key(Scalar::U32.key()));
    assert_eq!(radius.default, Some(Literal::Int(3)));
    assert_eq!(
        (radius.label, radius.unit, radius.group),
        ("Radius", "px", "g")
    );
    let amount = &Blur::FIELDS[1];
    assert_eq!(
        (amount.name, amount.advanced, amount.description),
        ("strength", true, "How much")
    );
    assert_eq!(Blur::FIELDS.len(), 3);
}

#[test]
fn limits_are_recorded_for_the_planner() {
    assert_eq!(
        Blur::RULES,
        [FieldRule {
            field: "radius",
            min: Some(Literal::Int(1)),
            max: Some(Literal::Int(99)),
            odd: true,
            policy: "",
        }]
    );
    let bad = Blur {
        radius: 0,
        ..Blur::default()
    };
    assert_eq!(Blur::CHECKERS[0](&bad), Err("radius must be positive"));
}

#[test]
fn values_come_from_literals_and_then_defaults() {
    let rows = [("radius", Literal::Int(7))];
    let built = Blur::from_literals(lookup(&rows)).unwrap();
    assert_eq!((built.radius, built.amount, built.plain), (7, 0.5, false));
    let wrong = [("radius", Literal::Bool(true))];
    assert_eq!(
        Blur::from_literals(lookup(&wrong)).err().unwrap().code,
        Code::InvalidLiteral
    );
}

#[derive(Type)]
#[type_key("acme:point")]
struct Point {
    x: u32,
    y: u8,
}

#[derive(Type)]
#[type_key("acme:shape")]
enum Shape {
    Empty,
    Radius(u32),
    Corners { a: Point2, b: Point2 },
}

#[derive(Type)]
struct Point2(u8, u8);

#[test]
fn a_struct_describes_its_fields() {
    assert_eq!(Point::KEY, TypeKey::new("acme:point"));
    assert_eq!(
        Point::EXPR,
        TypeExpr::Struct(&[
            Field {
                name: "x",
                ty: TypeExpr::Scalar(Scalar::U32)
            },
            Field {
                name: "y",
                ty: TypeExpr::Scalar(Scalar::U8)
            },
        ])
    );
}

#[test]
fn an_enum_describes_its_variants() {
    let TypeExpr::Enum(variants) = Shape::EXPR else {
        panic!()
    };
    assert_eq!(
        variants[0],
        Variant {
            name: "Empty",
            payload: None
        }
    );
    assert_eq!(variants[1].payload, Some(TypeExpr::Scalar(Scalar::U32)));
    assert!(matches!(variants[2].payload, Some(TypeExpr::Struct(fields)) if fields.len() == 2));
}

#[test]
fn the_default_key_is_the_lowercased_path() {
    assert_eq!(Point2::KEY.as_str(), "rust:derived::point2");
    assert_eq!(
        Sample::KEY.as_str(),
        "rust:daedalus_author_fixture::sample::sample"
    );
    assert!(matches!(Sample::EXPR, TypeExpr::Struct(fields) if fields.len() == 2));
}
