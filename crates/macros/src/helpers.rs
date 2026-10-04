use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::Span;
use quote::{ToTokens, quote};
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{
    Expr, ExprUnary, Lit, LitFloat, LitInt, LitStr, Meta, MetaNameValue, Path, PathSegment,
    ReturnType, Token, Type, UnOp,
};

pub fn compile_error(message: String) -> proc_macro2::TokenStream {
    quote! { ::core::compile_error!(#message); }
}

/// A Daedalus member crate referenced from generated code.
#[derive(Clone, Copy)]
pub enum DaedalusCrate {
    Core,
    Data,
    Gpu,
    Registry,
    Runtime,
    Transport,
}

impl DaedalusCrate {
    /// Path to the crate as seen from the crate being expanded.
    ///
    /// Resolves through the facade (`::daedalus::<via>`) when the consumer depends on
    /// `daedalus-rs`, otherwise through the member crate itself (e.g. `::daedalus_data`).
    pub fn path(self) -> proc_macro2::TokenStream {
        let (pkg, via) = match self {
            Self::Core => ("daedalus-core", "core"),
            Self::Data => ("daedalus-data", "data"),
            Self::Gpu => ("daedalus", "gpu"),
            Self::Registry => ("daedalus-registry", "registry"),
            Self::Runtime => ("daedalus-runtime", "runtime"),
            Self::Transport => ("daedalus-transport", "transport"),
        };
        let found_name = |found| match found {
            FoundCrate::Itself => None,
            FoundCrate::Name(name) => Some(name),
        };
        let facade = crate_name("daedalus-rs")
            .or_else(|_| crate_name("daedalus"))
            .ok()
            .map(|found| {
                found_name(found)
                    .filter(|name| name != "daedalus_rs")
                    .unwrap_or_else(|| "daedalus".to_string())
            });
        if let Some(root) = facade {
            let root = syn::Ident::new(&root, Span::call_site());
            let via = syn::Ident::new(via, Span::call_site());
            return quote! { ::#root::#via };
        }
        let name = crate_name(pkg)
            .ok()
            .and_then(found_name)
            .unwrap_or_else(|| pkg.replace('-', "_"));
        let ident = syn::Ident::new(&name, Span::call_site());
        quote! { ::#ident }
    }
}

/// A `&'static str` argument: a string literal, a path to a `const`/`static` string, or any other
/// expression such as `concat!(...)` or a user macro. The type checker validates non-literal forms;
/// only literals of another type are rejected here.
pub fn str_expr(expr: &Expr, what: &str) -> Result<Expr, proc_macro2::TokenStream> {
    match lit_from_expr(expr) {
        Some(Lit::Str(_)) | None => Ok(expr.clone()),
        Some(_) => Err(compile_error(format!(
            "{what} must be a string literal or an expression evaluating to `&'static str`"
        ))),
    }
}

/// A string literal argument.
pub fn lit_str_arg(expr: &Expr, what: &str) -> Result<LitStr, proc_macro2::TokenStream> {
    match lit_from_expr(expr) {
        Some(Lit::Str(lit)) => Ok(lit),
        _ => Err(compile_error(format!("{what} must be a string literal"))),
    }
}

/// A function path argument.
pub fn fn_path_arg(expr: Expr, what: &str) -> Result<Path, proc_macro2::TokenStream> {
    match expr {
        Expr::Path(path) => Ok(path.path),
        _ => Err(compile_error(format!("{what} must be a function path"))),
    }
}

/// Last path segment of a plain (non-qualified) path type.
pub fn last_segment(ty: &Type) -> Option<&PathSegment> {
    match ty {
        Type::Path(p) if p.qself.is_none() => p.path.segments.last(),
        _ => None,
    }
}

/// Whether the last path segment of a plain path type is `name`.
pub fn last_ident_is(ty: &Type, name: &str) -> bool {
    last_segment(ty).is_some_and(|seg| seg.ident == name)
}

/// The referenced type of `&T` / `&mut T`, or `ty` itself.
pub fn strip_ref(ty: &Type) -> &Type {
    match ty {
        Type::Reference(r) => &r.elem,
        _ => ty,
    }
}

/// The `idx`-th generic type argument of a path segment (`Device<A, B>` gives `B` for 1).
pub fn segment_type_arg(seg: &PathSegment, idx: usize) -> Option<&Type> {
    let syn::PathArguments::AngleBracketed(args) = &seg.arguments else {
        return None;
    };
    match args.args.iter().nth(idx)? {
        syn::GenericArgument::Type(ty) => Some(ty),
        _ => None,
    }
}

/// The `idx`-th generic type argument of `ty` when its last path segment is `name`.
pub fn generic_arg<'a>(ty: &'a Type, name: &str, idx: usize) -> Option<&'a Type> {
    let seg = last_segment(ty)?;
    if seg.ident != name {
        return None;
    }
    segment_type_arg(seg, idx)
}

/// `T` of a `-> Result<T, _>` return type.
pub fn result_ok_type(ret: &ReturnType) -> Option<&Type> {
    let ReturnType::Type(_, ty) = ret else {
        return None;
    };
    generic_arg(ty, "Result", 0)
}

pub fn arc_inner_type(ty: &Type) -> Option<&Type> {
    generic_arg(ty, "Arc", 0)
}

pub fn is_unit_type(ty: &Type) -> bool {
    matches!(ty, Type::Tuple(t) if t.elems.is_empty())
}

#[derive(Clone)]
#[expect(
    clippy::large_enum_variant,
    reason = "proc-macro parse nodes are short-lived and keeping direct syn patterns avoids boxing churn"
)]
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
        let Meta::List(list) = &attr.meta else {
            continue;
        };
        let Ok(items) = parse_nested(list) else {
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

/// Registers the default const coercer of `ty` into `coercers` (a `&ConstCoercerMap`) when a
/// plugin installs; see `daedalus_runtime::const_coerce`.
pub fn const_coercer_registration(
    ty: &Type,
    coercers: &proc_macro2::TokenStream,
    runtime_crate: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let support = quote! { #runtime_crate::const_coerce::derive_support };
    quote! {
        {
            // Only the trait matching each probe is used; which one depends on the type.
            #[allow(unused_imports)]
            use #support::{
                NoSchemaCoerce as _, NoSerdeCoerce as _, SchemaCoerce as _, SerdeCoerce as _,
            };
            let __probe = &#support::Probe::<#ty>(::core::marker::PhantomData);
            #runtime_crate::const_coerce::register_default_const_coercer::<#ty>(
                #coercers,
                __probe.schema_coercer(),
                __probe.serde_coercer(),
            );
        }
    }
}

/// Whether any token of `tokens`, including those nested in groups, matches `pred`.
pub fn any_token(
    tokens: proc_macro2::TokenStream,
    pred: &dyn Fn(&proc_macro2::TokenTree) -> bool,
) -> bool {
    tokens.into_iter().any(|token| match &token {
        proc_macro2::TokenTree::Group(group) => any_token(group.stream(), pred),
        _ => pred(&token),
    })
}
