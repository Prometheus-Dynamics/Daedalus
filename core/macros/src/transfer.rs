//! `#[transfer]`: a memory handler in one function.

use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote, quote_spanned};
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{Error, Expr, FnArg, Ident, ItemFn, Pat, Result, ReturnType, Token, Type};

use crate::args::NodeArgs;
use crate::args::{BackendArg, parse_backend, value};
use crate::emit::error_check;
use crate::ports::{checked_key, generic_of};

#[derive(Default)]
pub(crate) struct TransferArgs {
    from: Option<Expr>,
    to: Option<Expr>,
    cost: Option<Ident>,
    backend: Option<BackendArg>,
}

impl Parse for TransferArgs {
    fn parse(input: ParseStream<'_>) -> Result<TransferArgs> {
        let mut args = TransferArgs::default();
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            match key.to_string().as_str() {
                "from" => args.from = Some(value(input)?),
                "to" => args.to = Some(value(input)?),
                "cost" => args.cost = Some(value(input)?),
                "backend" => args.backend = Some(parse_backend(input)?),
                _ => return Err(Error::new(key.span(), "unsupported #[transfer] argument")),
            }
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }
        Ok(args)
    }
}

fn class_of(cost: &Ident) -> Result<TokenStream> {
    let variant = match cost.to_string().as_str() {
        "handoff" => quote!(Handoff),
        "import" => quote!(Import),
        "copy" => quote!(Copy),
        _ => {
            return Err(Error::new(
                cost.span(),
                "cost must be handoff, import or copy",
            ));
        }
    };
    Ok(quote!(::daedalus_registry::TransferClass::#variant))
}

fn required<'a, T>(value: &'a Option<T>, name: &str) -> Result<&'a T> {
    value
        .as_ref()
        .ok_or_else(|| Error::new(Span::call_site(), format!("#[transfer] needs `{name}`")))
}

/// The value type of the input: `&T` and `T` both read `T`.
fn input_type(item: &ItemFn) -> Result<Type> {
    let Some(FnArg::Typed(param)) = item.sig.inputs.first() else {
        return Err(Error::new_spanned(
            &item.sig.ident,
            "a transfer takes the value to move",
        ));
    };
    Ok(match &*param.ty {
        Type::Reference(reference) => (*reference.elem).clone(),
        other => other.clone(),
    })
}

fn params(item: &ItemFn) -> Result<TokenStream> {
    if item.sig.inputs.len() > 2 {
        return Err(Error::new_spanned(
            &item.sig.inputs,
            "a transfer takes the value and, optionally, the destination's context",
        ));
    }
    let mut tokens = TokenStream::new();
    for param in &item.sig.inputs {
        let FnArg::Typed(param) = param else {
            return Err(Error::new_spanned(param, "a transfer can't take self"));
        };
        let Pat::Ident(pattern) = &*param.pat else {
            return Err(Error::new_spanned(
                &param.pat,
                "a transfer parameter needs a plain name",
            ));
        };
        let (name, ty) = (&pattern.ident, &param.ty);
        tokens.extend(quote!(#name: #ty,));
    }
    Ok(tokens)
}

pub(crate) fn expand(args: TransferArgs, item: ItemFn) -> Result<TokenStream> {
    let (from, to) = (required(&args.from, "from")?, required(&args.to, "to")?);
    let class = class_of(required(&args.cost, "cost")?)?;
    let ReturnType::Type(_, returned) = &item.sig.output else {
        return Err(Error::new_spanned(
            &item.sig.ident,
            "a transfer returns the moved value",
        ));
    };
    let mut output = (**returned).clone();
    let mut error = None;
    if let Some(parts) = generic_of(&output, "Result") {
        error = parts.get(1).map(|ty| (*ty).clone());
        output = parts.first().map_or(output.clone(), |ty| (*ty).clone());
    }
    let result_check = error_check(&error);
    let input = input_type(&item)?;
    let (key, output_key) = (checked_key(&input), checked_key(&output));
    let params = params(&item)?;
    let name = &item.sig.ident;
    let id = name.to_string();
    let record = format_ident!("{}_TRANSFER", id.to_uppercase());
    let (path, backend_args) = match &args.backend {
        Some(backend) => {
            let path = &backend.path;
            (quote_spanned!(path.span()=> #path), backend.args.clone())
        }
        None => (quote!(::daedalus_cpu_author), TokenStream::new()),
    };
    let ret = returned;
    let bound = crate::ports::shape(&NodeArgs::default(), None, None, &item.sig, None)?;
    let bind = crate::emit::bind_tokens(None, None, &bound);
    Ok(quote! {
        #item
        #result_check

        pub static #record: ::daedalus_registry::TransferRecord = ::daedalus_registry::TransferRecord {
            id: ::core::concat!(::core::module_path!(), "::", #id),
            at: ::daedalus_registry::SourceAt(
                ::core::concat!(::core::file!(), ":", ::core::line!())
            ),
            key: #key,
            output: #output_key,
            from: #from,
            to: #to,
            class: #class,
            body: #path::__daedalus_body!(
                kind = transfer,
                name = (#name),
                args = (#backend_args),
                params = (#params),
                ret = #ret,
                bind = #bind,
                decl = #record
            ),
        };
    })
}
