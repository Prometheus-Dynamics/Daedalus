use syn::{Lit, LitStr};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum NumberKind {
    Int,
    Float,
}

pub(super) fn number_kind(ty: &syn::Type) -> Option<NumberKind> {
    match ty {
        syn::Type::Path(p) if p.qself.is_none() => {
            let ident = p.path.segments.last()?.ident.to_string();
            match ident.as_str() {
                "f32" | "f64" => Some(NumberKind::Float),
                "i8" | "i16" | "i32" | "i64" | "i128" | "isize" | "u8" | "u16" | "u32" | "u64"
                | "u128" | "usize" => Some(NumberKind::Int),
                _ => None,
            }
        }
        syn::Type::Reference(r) => number_kind(&r.elem),
        _ => None,
    }
}

pub(super) struct PortSpec {
    pub(super) field_ident: syn::Ident,
    pub(super) field_ty: syn::Type,
    pub(super) name: LitStr,
    pub(super) source: Option<LitStr>,
    pub(super) description: Option<LitStr>,
    /// Display metadata for a UI: a human label, a unit, a group (stage or section) and whether
    /// the port is an expert knob (`inputs.<port>.label` / `.unit` / `.group` / `.advanced`).
    pub(super) label: Option<LitStr>,
    pub(super) unit: Option<LitStr>,
    pub(super) group: Option<LitStr>,
    pub(super) advanced: Option<bool>,
    pub(super) default_value: Option<Lit>,
    pub(super) min_value: Option<Lit>,
    pub(super) max_value: Option<Lit>,
    pub(super) odd: bool,
    pub(super) policy: Option<LitStr>,
    pub(super) ty_override: Option<proc_macro2::TokenStream>,
    pub(super) meta: Vec<(LitStr, Lit)>,
}
