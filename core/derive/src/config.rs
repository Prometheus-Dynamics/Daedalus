//! `#[derive(NodeConfig)]`.

use proc_macro2::{Span, TokenStream};
use quote::{quote, quote_spanned};
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{
    Data, DeriveInput, Error, Expr, Fields, Ident, Lit, LitBool, LitStr, Path, Result, Token, Type,
    UnOp,
};

use crate::local_key_check;

#[derive(Default)]
struct PortAttr {
    name: Option<LitStr>,
    source: Option<LitStr>,
    description: Option<LitStr>,
    label: Option<LitStr>,
    unit: Option<LitStr>,
    group: Option<LitStr>,
    policy: Option<LitStr>,
    advanced: bool,
    odd: bool,
    default: Option<Expr>,
    min: Option<Expr>,
    max: Option<Expr>,
    ty: Option<Type>,
}

fn value<T: Parse>(input: ParseStream<'_>) -> Result<T> {
    input.parse::<Token![=]>()?;
    input.parse()
}

fn flag(input: ParseStream<'_>) -> Result<bool> {
    if input.peek(Token![=]) {
        Ok(value::<LitBool>(input)?.value)
    } else {
        Ok(true)
    }
}

impl Parse for PortAttr {
    fn parse(input: ParseStream<'_>) -> Result<PortAttr> {
        let mut port = PortAttr::default();
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            match key.to_string().as_str() {
                "name" => port.name = Some(value(input)?),
                "source" => port.source = Some(value(input)?),
                "description" => port.description = Some(value(input)?),
                "label" => port.label = Some(value(input)?),
                "unit" => port.unit = Some(value(input)?),
                "group" => port.group = Some(value(input)?),
                "policy" => port.policy = Some(value(input)?),
                "advanced" => port.advanced = flag(input)?,
                "odd" => port.odd = flag(input)?,
                "default" => port.default = Some(value(input)?),
                "min" => port.min = Some(value(input)?),
                "max" => port.max = Some(value(input)?),
                "ty" => port.ty = Some(value(input)?),
                _ => return Err(Error::new(key.span(), "unsupported #[port] argument")),
            }
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }
        Ok(port)
    }
}

/// `#[validate(fn = path)]`.
struct ValidateAttr(Path);

impl Parse for ValidateAttr {
    fn parse(input: ParseStream<'_>) -> Result<ValidateAttr> {
        input.parse::<Token![fn]>()?;
        Ok(ValidateAttr(value(input)?))
    }
}

fn text(value: &Option<LitStr>) -> TokenStream {
    match value {
        Some(value) => quote!(#value),
        None => quote!(""),
    }
}

fn literal(expr: &Expr) -> Result<TokenStream> {
    let path = quote!(::daedalus_registry::__private::Literal);
    let unsupported = || Error::new_spanned(expr, "expected a bool, number or string");
    let (lit, negative) = match expr {
        Expr::Lit(lit) => (&lit.lit, false),
        Expr::Unary(unary) if matches!(unary.op, UnOp::Neg(_)) => match &*unary.expr {
            Expr::Lit(lit) => (&lit.lit, true),
            _ => return Err(unsupported()),
        },
        _ => return Err(unsupported()),
    };
    let sign = negative.then(|| quote!(-));
    Ok(match lit {
        Lit::Bool(value) if !negative => quote!(#path::Bool(#value)),
        Lit::Int(value) => {
            let value = value.base10_parse::<i64>()?;
            quote!(#path::Int(#sign #value))
        }
        Lit::Float(value) => {
            let value = value.base10_parse::<f64>()?;
            quote!(#path::Float(#sign #value))
        }
        Lit::Str(value) if !negative => quote!(#path::Str(#value)),
        _ => return Err(unsupported()),
    })
}

fn optional_literal(expr: &Option<Expr>) -> Result<TokenStream> {
    Ok(match expr {
        Some(expr) => {
            let value = literal(expr)?;
            quote!(::core::option::Option::Some(#value))
        }
        None => quote!(::core::option::Option::None),
    })
}

pub(crate) fn derive(input: DeriveInput) -> Result<TokenStream> {
    let name = &input.ident;
    if !input.generics.params.is_empty() {
        return Err(Error::new_spanned(
            &input.generics,
            "#[derive(NodeConfig)] takes no generics",
        ));
    }
    let Data::Struct(data) = &input.data else {
        return Err(Error::new(
            Span::call_site(),
            "#[derive(NodeConfig)] goes on a struct",
        ));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(Error::new(
            Span::call_site(),
            "#[derive(NodeConfig)] needs named fields",
        ));
    };
    let p = quote!(::daedalus_registry::__private);
    let r = quote!(::daedalus_registry);
    let mut ports = Vec::new();
    let mut rules = Vec::new();
    let mut reads = Vec::new();
    let mut checks = TokenStream::new();
    for field in &fields.named {
        let ident = field.ident.as_ref().expect("a named field");
        let field_ty = &field.ty;
        let mut port = PortAttr::default();
        for attr in &field.attrs {
            if attr.path().is_ident("port") {
                port = attr.parse_args()?;
            }
        }
        let port_name = port.name.as_ref().map_or(ident.to_string(), LitStr::value);
        let key_ty = port.ty.as_ref().unwrap_or(field_ty);
        checks.extend(local_key_check(key_ty));
        let default = optional_literal(&port.default)?;
        let (label, unit, group) = (text(&port.label), text(&port.unit), text(&port.group));
        let (description, source) = (text(&port.description), text(&port.source));
        let advanced = port.advanced;
        ports.push(quote! {
            #r::PortDecl {
                name: #port_name,
                ty: #r::port_type::<#key_ty>(),
                access: #p::Access::Read,
                optional: false,
                conditional: false,
                default: #default,
                label: #label,
                unit: #unit,
                group: #group,
                advanced: #advanced,
                description: #description,
                source: #source,
            }
        });
        if port.min.is_some() || port.max.is_some() || port.odd || port.policy.is_some() {
            let (min, max) = (optional_literal(&port.min)?, optional_literal(&port.max)?);
            let (odd, policy) = (port.odd, text(&port.policy));
            rules.push(quote! {
                #r::FieldRule { field: #port_name, min: #min, max: #max, odd: #odd, policy: #policy }
            });
        }
        let absent = match &port.default {
            Some(expr) => {
                let value = literal(expr)?;
                quote!(<#field_ty as #p::FromLiteral>::from_literal(&#value)?)
            }
            None => {
                quote_spanned!(field_ty.span()=> <#field_ty as ::core::default::Default>::default())
            }
        };
        reads.push(quote! {
            #ident: match lookup(#port_name) {
                ::core::option::Option::Some(value) => {
                    <#field_ty as #p::FromLiteral>::from_literal(value)?
                }
                ::core::option::Option::None => #absent,
            }
        });
    }
    let mut checkers = Vec::new();
    for attr in &input.attrs {
        if attr.path().is_ident("validate") {
            let ValidateAttr(path) = attr.parse_args()?;
            checkers.push(path);
        }
    }
    let bounds = quote_spanned! {name.span()=>
        const _: () = {
            const fn config_must_be_clone_send_sync_static<
                T: ::core::clone::Clone + ::core::marker::Send + ::core::marker::Sync + 'static,
            >() {}
            config_must_be_clone_send_sync_static::<#name>()
        };
    };
    Ok(quote! {
        #bounds
        #checks

        impl #name {
            /// One entry per field.
            pub const FIELDS: &'static [#r::PortDecl] = &[#(#ports),*];
            /// Limits on fields, for the planner to enforce.
            pub const RULES: &'static [#r::FieldRule] = &[#(#rules),*];
            /// Whole-value checks, for the planner to run.
            pub const CHECKERS: &'static [fn(&Self) -> ::core::result::Result<(), &'static str>] =
                &[#(#checkers),*];

            /// Builds the value. A field `lookup` doesn't find takes its default.
            pub fn from_literals<'a>(
                lookup: impl ::core::ops::Fn(&str) -> ::core::option::Option<&'a #p::Literal>,
            ) -> ::core::result::Result<Self, #p::Error> {
                ::core::result::Result::Ok(Self { #(#reads),* })
            }
        }
    })
}
