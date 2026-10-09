use proc_macro::TokenStream;
use quote::{ToTokens, quote};
use syn::{Data, DeriveInput, Fields, Lit, parse_macro_input};

use crate::helpers::{DaedalusCrate, any_token, compile_error, const_coercer_registration};

mod codegen;
mod model;
mod parse;

use codegen::{metadata_entries, port_decl_tokens, sanitize_field_tokens};
use model::{PortSpec, number_kind};
use parse::{parse_port_spec, parse_validate_fn};

pub fn node_config(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let struct_ident = input.ident.clone();
    let generics = input.generics.clone();
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let runtime_crate = DaedalusCrate::Runtime.path();
    let registry_crate = DaedalusCrate::Registry.path();
    let data_crate = DaedalusCrate::Data.path();

    let validate_fn = match parse_validate_fn(&input.attrs) {
        Ok(validate_fn) => validate_fn,
        Err(error) => return TokenStream::from(error),
    };

    let fields = match input.data {
        Data::Struct(ds) => ds.fields,
        _ => {
            return TokenStream::from(compile_error(
                "NodeConfig can only be derived for structs".into(),
            ));
        }
    };
    let named_fields = match fields {
        Fields::Named(named) => named.named,
        _ => {
            return TokenStream::from(compile_error(
                "NodeConfig requires named struct fields".into(),
            ));
        }
    };

    let generic_type_params: ::std::collections::HashSet<::std::string::String> = input
        .generics
        .type_params()
        .map(|tp| tp.ident.to_string())
        .collect();

    let mut specs: Vec<PortSpec> = Vec::new();
    for field in named_fields {
        match parse_port_spec(field) {
            Ok(spec) => specs.push(spec),
            Err(error) => return TokenStream::from(error),
        }
    }

    let mut errors: Vec<proc_macro2::TokenStream> = Vec::new();
    for spec in &specs {
        let needs_numeric = spec.min_value.is_some() || spec.max_value.is_some() || spec.odd;
        if needs_numeric && number_kind(&spec.field_ty).is_none() {
            errors.push(compile_error(format!(
                "port `{}` uses numeric constraints but field type is not numeric",
                spec.field_ident
            )));
        }
        if let Some(Lit::Bool(_)) = spec.min_value {
            errors.push(compile_error("min must be an int/float literal".into()));
        }
        if let Some(Lit::Bool(_)) = spec.max_value {
            errors.push(compile_error("max must be an int/float literal".into()));
        }
        if let Some(policy) = &spec.policy
            && !matches!(policy.value().as_str(), "clamp" | "error")
        {
            errors.push(compile_error(
                "policy must be \"clamp\" or \"error\"".into(),
            ));
        }
    }
    if !errors.is_empty() {
        return TokenStream::from(quote! { #(#errors)* });
    }

    let ports_tokens: Vec<proc_macro2::TokenStream> = specs
        .iter()
        .map(|spec| {
            port_decl_tokens(
                spec,
                &generic_type_params,
                &runtime_crate,
                &registry_crate,
                &data_crate,
            )
        })
        .collect();

    let metadata_tokens: Vec<proc_macro2::TokenStream> = specs
        .iter()
        .flat_map(|spec| metadata_entries(spec, &data_crate))
        .collect();

    let from_io_fields: Vec<proc_macro2::TokenStream> = specs
        .iter()
        .map(|spec| {
            let ident = &spec.field_ident;
            let ty = &spec.field_ty;
            let name = &spec.name;
            quote! {
                let #ident = io
                    .get_typed::<#ty>(#name)
                    .ok_or_else(|| #runtime_crate::NodeError::missing_input(#name))?;
            }
        })
        .collect();

    let sanitize_fields: Vec<proc_macro2::TokenStream> = specs
        .iter()
        .enumerate()
        .map(|(idx, spec)| sanitize_field_tokens(idx, spec, &runtime_crate, &data_crate))
        .collect();

    let validate_call = validate_fn.as_ref().map(|path| {
        quote! {
            #path(self)?;
        }
    });

    // Field types naming a generic parameter cannot be probed for a coercer.
    let coercers = quote! { coercers };
    let register_coercers = specs.iter().filter_map(|spec| {
        let generic = any_token(spec.field_ty.to_token_stream(), &|token| {
            matches!(token, proc_macro2::TokenTree::Ident(i) if generic_type_params.contains(&i.to_string()))
        });
        (!generic)
            .then(|| const_coercer_registration(&spec.field_ty, &coercers, &runtime_crate))
    });

    let port_names = specs.iter().map(|spec| &spec.name);
    let struct_fields: Vec<syn::Ident> =
        specs.iter().map(|spec| spec.field_ident.clone()).collect();

    TokenStream::from(quote! {
        impl #impl_generics #runtime_crate::config::NodeConfig for #struct_ident #ty_generics #where_clause {
            fn ports(
                __types: &#data_crate::typing::TypeRegistry,
            ) -> Vec<#registry_crate::capability::PortDecl> {
                vec![#(#ports_tokens),*]
            }

            fn port_names() -> &'static [&'static str] {
                &[#(#port_names),*]
            }

            fn metadata() -> ::std::collections::BTreeMap<String, #data_crate::model::Value> {
                let mut __meta: ::std::collections::BTreeMap<String, #data_crate::model::Value> =
                    ::std::collections::BTreeMap::new();
                #(#metadata_tokens)*
                __meta
            }

            fn from_io(io: &#runtime_crate::NodeIo) -> Result<Self, #runtime_crate::NodeError> {
                #(#from_io_fields)*
                Ok(Self { #(#struct_fields),* })
            }

            fn sanitize(self) -> Result<#runtime_crate::config::Sanitized<Self>, #runtime_crate::config::ConfigError> {
                let mut changes = Vec::new();
                #(#sanitize_fields)*
                Ok(#runtime_crate::config::Sanitized {
                    value: Self { #(#struct_fields),* },
                    changes,
                })
            }

            fn validate(&self) -> Result<(), #runtime_crate::config::ConfigError> {
                #validate_call
                Ok(())
            }

            fn register_const_coercers(coercers: &#runtime_crate::io::ConstCoercerMap) {
                #(#register_coercers)*
                let _ = coercers;
            }
        }
    })
}
