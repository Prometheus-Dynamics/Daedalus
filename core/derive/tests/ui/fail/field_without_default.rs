use daedalus_derive::NodeConfig;

#[derive(Clone, NodeConfig)]
pub struct Settings {
    pub inner: NoDefault,
}

#[derive(Clone)]
pub struct NoDefault;

impl daedalus_registry::__private::Typed for NoDefault {
    const KEY: daedalus_types::TypeKey = daedalus_types::TypeKey::new("acme:nodefault");
    const EXPR: daedalus_types::TypeExpr = daedalus_types::TypeExpr::Opaque(Self::KEY);
}

impl daedalus_types::FromLiteral for NoDefault {
    fn from_literal(_: &daedalus_types::Literal) -> Result<Self, daedalus_types::Error> {
        Ok(NoDefault)
    }
}

fn main() {}
