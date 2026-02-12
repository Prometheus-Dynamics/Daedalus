#![allow(clippy::doc_overindented_list_items)]
use proc_macro2::Span;
use quote::{ToTokens, quote};
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{
    Expr, ExprUnary, Lit, LitFloat, LitInt, LitStr, Meta, MetaList, MetaNameValue, Token, UnOp,
};

pub fn compile_error(message: String) -> proc_macro2::TokenStream {
    quote! { ::core::compile_error!(#message); }
}

#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
pub enum NestedMeta {
    Meta(syn::Meta),
    Lit(Lit),
}

impl Parse for NestedMeta {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if input.peek(Lit) {
            Ok(NestedMeta::Lit(input.parse()?))
        } else {
            Ok(NestedMeta::Meta(input.parse()?))
        }
    }
}

impl ToTokens for NestedMeta {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        match self {
            NestedMeta::Meta(meta) => meta.to_tokens(tokens),
            NestedMeta::Lit(lit) => lit.to_tokens(tokens),
        }
    }
}

pub type AttributeArgs = Punctuated<NestedMeta, Token![,]>;

pub fn parse_nested(list: &syn::MetaList) -> Result<Vec<NestedMeta>, proc_macro2::TokenStream> {
    list.parse_args_with(AttributeArgs::parse_terminated)
        .map(|items| items.into_iter().collect())
        .map_err(|err| compile_error(err.to_string()))
}

#[allow(dead_code)]
pub fn collect_list(
    list: &syn::MetaList,
    target: &mut Vec<LitStr>,
) -> Result<(), proc_macro2::TokenStream> {
    for nested in parse_nested(list)? {
        match nested {
            NestedMeta::Lit(Lit::Str(s)) => target.push(s),
            _ => return Err(compile_error("list entries must be string literals".into())),
        }
    }
    Ok(())
}

pub fn litstr_from_ident(id: &syn::Ident) -> LitStr {
    LitStr::new(&id.to_string(), Span::call_site())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SerdeRenameAll {
    Camel,
    Snake,
    Kebab,
    Pascal,
    Lower,
    Upper,
    ScreamingSnake,
}

fn parse_serde_string_kv(attrs: &[syn::Attribute], key: &str) -> Option<LitStr> {
    for attr in attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        let Meta::List(MetaList { .. }) = &attr.meta else {
            continue;
        };
        let Meta::List(list) = attr.meta.clone() else {
            continue;
        };
        let Ok(items) = parse_nested(&list) else {
            continue;
        };
        for item in items {
            let NestedMeta::Meta(Meta::NameValue(MetaNameValue { path, value, .. })) = item else {
                continue;
            };
            let Some(ident) = path.get_ident() else {
                continue;
            };
            if ident != key {
                continue;
            }
            let syn::Expr::Lit(expr_lit) = value else {
                continue;
            };
            let Lit::Str(s) = expr_lit.lit else {
                continue;
            };
            return Some(s);
        }
    }
    None
}

pub fn parse_serde_rename_all(attrs: &[syn::Attribute]) -> Option<SerdeRenameAll> {
    let raw = parse_serde_string_kv(attrs, "rename_all")?;
    match raw.value().as_str() {
        "camelCase" => Some(SerdeRenameAll::Camel),
        "snake_case" => Some(SerdeRenameAll::Snake),
        "kebab-case" => Some(SerdeRenameAll::Kebab),
        "PascalCase" => Some(SerdeRenameAll::Pascal),
        "lowercase" => Some(SerdeRenameAll::Lower),
        "UPPERCASE" => Some(SerdeRenameAll::Upper),
        "SCREAMING_SNAKE_CASE" => Some(SerdeRenameAll::ScreamingSnake),
        _ => None,
    }
}

pub fn parse_serde_rename(attrs: &[syn::Attribute]) -> Option<LitStr> {
    parse_serde_string_kv(attrs, "rename")
}

fn words(raw: &str) -> Vec<String> {
    if raw.contains('_') {
        return raw
            .split('_')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
    }
    if raw.contains('-') {
        return raw
            .split('-')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
    }

    // Best-effort split for CamelCase/PascalCase identifiers.
    let mut out: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut chars = raw.chars().peekable();
    while let Some(c) = chars.next() {
        let next = chars.peek().copied();
        let is_boundary = if cur.is_empty() {
            false
        } else if c.is_ascii_uppercase() {
            // `aB` or `1B` => boundary before B
            let prev = cur.chars().last().unwrap_or('_');
            prev.is_ascii_lowercase() || prev.is_ascii_digit()
                // `ABc` => boundary before B? (keep acronym together)
                || (prev.is_ascii_uppercase() && next.is_some_and(|n| n.is_ascii_lowercase()))
        } else {
            false
        };

        if is_boundary {
            out.push(std::mem::take(&mut cur));
        }
        cur.push(c);
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    if out.is_empty() {
        vec![raw.to_string()]
    } else {
        out
    }
}

fn to_snake_case(raw: &str) -> String {
    words(raw)
        .into_iter()
        .map(|w| w.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join("_")
}

fn to_kebab_case(raw: &str) -> String {
    words(raw)
        .into_iter()
        .map(|w| w.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join("-")
}

fn capitalize(raw: &str) -> String {
    let mut chars = raw.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    let mut out = String::new();
    out.push(first.to_ascii_uppercase());
    out.push_str(&chars.as_str().to_ascii_lowercase());
    out
}

fn to_camel_case(raw: &str) -> String {
    let ws = words(raw);
    if ws.is_empty() {
        return raw.to_string();
    }
    let mut out = String::new();
    out.push_str(&ws[0].to_ascii_lowercase());
    for w in ws.iter().skip(1) {
        out.push_str(&capitalize(w));
    }
    out
}

fn to_pascal_case(raw: &str) -> String {
    words(raw).into_iter().map(|w| capitalize(&w)).collect()
}

pub fn apply_serde_rename_all(raw: &str, rule: SerdeRenameAll) -> String {
    match rule {
        SerdeRenameAll::Camel => to_camel_case(raw),
        SerdeRenameAll::Snake => to_snake_case(raw),
        SerdeRenameAll::Kebab => to_kebab_case(raw),
        SerdeRenameAll::Pascal => to_pascal_case(raw),
        SerdeRenameAll::Lower => raw.to_ascii_lowercase(),
        SerdeRenameAll::Upper => raw.to_ascii_uppercase(),
        SerdeRenameAll::ScreamingSnake => to_snake_case(raw).to_ascii_uppercase(),
    }
}

pub fn serde_name_for_ident(
    ident: &syn::Ident,
    attrs: &[syn::Attribute],
    rename_all: Option<SerdeRenameAll>,
) -> LitStr {
    if let Some(rename) = parse_serde_rename(attrs) {
        return rename;
    }
    let raw = ident.to_string();
    let cooked = rename_all
        .map(|rule| apply_serde_rename_all(&raw, rule))
        .unwrap_or(raw);
    LitStr::new(&cooked, Span::call_site())
}

pub fn lit_from_expr(expr: &syn::Expr) -> Option<Lit> {
    match expr {
        Expr::Lit(expr_lit) => Some(expr_lit.lit.clone()),
        Expr::Unary(ExprUnary {
            op: UnOp::Neg(_),
            expr,
            ..
        }) => {
            if let Expr::Lit(expr_lit) = &**expr {
                match &expr_lit.lit {
                    Lit::Int(i) => Some(Lit::Int(LitInt::new(&format!("-{}", i), i.span()))),
                    Lit::Float(f) => Some(Lit::Float(LitFloat::new(&format!("-{}", f), f.span()))),
                    _ => None,
                }
            } else {
                None
            }
        }
        _ => None,
    }
}
