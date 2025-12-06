#![allow(clippy::doc_overindented_list_items)]
use proc_macro2::Span;
use quote::{ToTokens, quote};
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{Expr, ExprUnary, Lit, LitFloat, LitInt, LitStr, Token, UnOp};

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

pub fn lit_from_expr(expr: &syn::Expr) -> Option<Lit> {
    match expr {
        Expr::Lit(expr_lit) => Some(expr_lit.lit.clone()),
        Expr::Unary(ExprUnary { op: UnOp::Neg(_), expr, .. }) => {
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
