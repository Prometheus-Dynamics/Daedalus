use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::Span;
use quote::quote;
use syn::{Expr, ItemStruct, Lit, LitStr, Meta, MetaList, MetaNameValue, Path, parse_macro_input};

use crate::helpers::{AttributeArgs, NestedMeta, compile_error, lit_from_expr, parse_nested};

fn crate_path(
    pkg: &str,
    fallback: &str,
    via_root: Option<&str>,
    daedalus_root: &Option<String>,
) -> proc_macro2::TokenStream {
    if let Some(root) = daedalus_root
        && let Some(via) = via_root
    {
        let root_ident = syn::Ident::new(root, Span::call_site());
        let via_ident = syn::Ident::new(via, Span::call_site());
        return quote! { ::#root_ident::#via_ident };
    }
    let name = crate_name(pkg)
        .map(|found| match found {
            FoundCrate::Itself => pkg.replace('-', "_"),
            FoundCrate::Name(name) => name,
        })
        .unwrap_or_else(|_| fallback.to_string());
    let ident = syn::Ident::new(&name, Span::call_site());
    quote! { ::#ident }
}

struct PluginArgs {
    id: LitStr,
    deps: Vec<LitStr>,
    types: Vec<syn::Ident>,
    nodes: Vec<syn::Ident>,
    adapters: Vec<syn::Ident>,
    devices: Vec<syn::Ident>,
    parts: Vec<Path>,
    install: Option<Path>,
}

fn collect_ident_list(list: &MetaList) -> Result<Vec<syn::Ident>, proc_macro2::TokenStream> {
    let mut out = Vec::new();
    for item in parse_nested(list)? {
        match item {
            NestedMeta::Meta(Meta::Path(path)) => {
                let Some(ident) = path.get_ident() else {
                    return Err(compile_error(
                        "plugin list entries must be identifiers".into(),
                    ));
                };
                out.push(ident.clone());
            }
            _ => {
                return Err(compile_error(
                    "plugin list entries must be identifiers".into(),
                ));
            }
        }
    }
    Ok(out)
}

fn parse_args(args: AttributeArgs) -> Result<PluginArgs, proc_macro2::TokenStream> {
    let mut id = None;
    let mut deps = Vec::new();
    let mut types = Vec::new();
    let mut nodes = Vec::new();
    let mut adapters = Vec::new();
    let mut devices = Vec::new();
    let mut parts = Vec::new();
    let mut install = None;

    for arg in args {
        match arg {
            NestedMeta::Meta(Meta::NameValue(MetaNameValue { path, value, .. }))
                if path.is_ident("id") =>
            {
                let Some(Lit::Str(value)) = lit_from_expr(&value) else {
                    return Err(compile_error("plugin id must be a string literal".into()));
                };
                id = Some(value);
            }
            NestedMeta::Meta(Meta::NameValue(MetaNameValue { path, value, .. }))
                if path.is_ident("install") =>
            {
                let Expr::Path(path) = value else {
                    return Err(compile_error(
                        "plugin install must be a function path".into(),
                    ));
                };
                install = Some(path.path);
            }
            NestedMeta::Meta(Meta::List(list)) if list.path.is_ident("nodes") => {
                nodes = collect_ident_list(&list)?;
            }
            NestedMeta::Meta(Meta::List(list)) if list.path.is_ident("types") => {
                types = collect_ident_list(&list)?;
            }
            NestedMeta::Meta(Meta::List(list)) if list.path.is_ident("adapters") => {
                adapters = collect_ident_list(&list)?;
            }
            NestedMeta::Meta(Meta::List(list)) if list.path.is_ident("devices") => {
                devices = collect_ident_list(&list)?;
            }
            NestedMeta::Meta(Meta::List(list)) if list.path.is_ident("deps") => {
                for item in parse_nested(&list)? {
                    let NestedMeta::Lit(Lit::Str(dep)) = item else {
                        return Err(compile_error(
                            "plugin deps entries must be string literals".into(),
                        ));
                    };
                    deps.push(dep);
                }
            }
            NestedMeta::Meta(Meta::List(list)) if list.path.is_ident("parts") => {
                for item in parse_nested(&list)? {
                    let NestedMeta::Meta(Meta::Path(path)) = item else {
                        return Err(compile_error("plugin parts entries must be paths".into()));
                    };
                    parts.push(path);
                }
            }
            _ => {
                return Err(compile_error(
                    "plugin arguments must use `id = \"...\", install = setup, deps(...), parts(...), types(...), nodes(...), adapters(...), devices(...)`"
                        .into(),
                ));
            }
        }
    }

    Ok(PluginArgs {
        id: id.ok_or_else(|| compile_error("missing plugin id".into()))?,
        deps,
        types,
        nodes,
        adapters,
        devices,
        parts,
        install,
    })
}

pub fn plugin(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args with AttributeArgs::parse_terminated);
    let input = parse_macro_input!(item as ItemStruct);

    let parsed = match parse_args(args) {
        Ok(parsed) => parsed,
        Err(err) => return err.into(),
    };
    if !input.generics.params.is_empty() {
        return compile_error("plugin structs cannot be generic yet".into()).into();
    }

    let daedalus_root: Option<String> = crate_name("daedalus-rs")
        .or_else(|_| crate_name("daedalus"))
        .ok()
        .map(|found| match found {
            FoundCrate::Itself => "daedalus".to_string(),
            FoundCrate::Name(name) => {
                if name == "daedalus_rs" {
                    "daedalus".to_string()
                } else {
                    name
                }
            }
        });
    let runtime_crate = crate_path(
        "daedalus-runtime",
        "daedalus_runtime",
        Some("runtime"),
        &daedalus_root,
    );
    let registry_crate = crate_path(
        "daedalus-registry",
        "daedalus_registry",
        Some("registry"),
        &daedalus_root,
    );

    let ident = input.ident;
    let vis = input.vis;
    let id = parsed.id;
    let deps = parsed.deps;
    let types = parsed.types;
    let nodes = parsed.nodes;
    let adapters = parsed.adapters;
    let devices = parsed.devices;
    let parts = parsed.parts;
    let install = parsed.install;
    let node_fields = &nodes;
    let node_structs: Vec<syn::Ident> = nodes.iter().map(node_struct_ident).collect();
    let node_handle_tys: Vec<syn::Ident> = node_structs
        .iter()
        .map(|node| syn::Ident::new(&format!("{node}Handle"), node.span()))
        .collect();
    let register_adapters: Vec<syn::Ident> = adapters
        .iter()
        .map(|adapter| syn::Ident::new(&format!("register_{adapter}_adapter"), adapter.span()))
        .collect();
    let register_devices: Vec<syn::Ident> = devices
        .iter()
        .map(|device| syn::Ident::new(&format!("register_{device}_device"), device.span()))
        .collect();
    let register_types: Vec<syn::Ident> = types
        .iter()
        .map(|ty| {
            syn::Ident::new(
                &format!("register_{}_type", snake_case(&ty.to_string())),
                ty.span(),
            )
        })
        .collect();
    let node_methods: Vec<syn::Ident> = nodes
        .iter()
        .map(|node| syn::Ident::new(&format!("node_{node}"), node.span()))
        .collect();
    let install_hook = install
        .as_ref()
        .map(|path| quote! { #path(registry)?; })
        .unwrap_or_default();

    let expanded = quote! {
        #[derive(Clone, Debug)]
        #vis struct #ident {
            #(pub #node_fields: #node_handle_tys),*
        }

        impl #ident {
            pub fn new() -> Self {
                Self {
                    #(#node_fields: #node_structs::handle().with_prefix(#id)),*
                }
            }

            #(
                pub fn #node_methods(&self) -> #node_handle_tys {
                    #node_structs::handle().with_prefix(#id)
                }
            )*

            pub fn install(
                &self,
                registry: &mut #runtime_crate::plugins::PluginInstallContext<'_>,
            ) -> #runtime_crate::plugins::PluginResult<()> {
                #(
                    registry.dependency(#deps);
                )*
                #install_hook
                #(
                    #runtime_crate::plugins::PluginPart::install_part(&#parts, registry)?;
                )*
                #(
                    #register_types(registry)?;
                )*
                #(
                    #register_adapters(registry)?;
                )*
                #(
                    #register_devices(registry)?;
                )*
                #(
                    for __contract in #node_structs::boundary_contracts()? {
                        registry.boundary_contract(__contract)?;
                    }
                )*
                #(
                    registry.merge::<#node_structs>()?;
                )*
                Ok(())
            }
        }

        impl Default for #ident {
            fn default() -> Self {
                Self::new()
            }
        }

        impl #runtime_crate::plugins::Plugin for #ident {
            fn id(&self) -> &'static str {
                #id
            }

            fn manifest(&self) -> #registry_crate::capability::PluginManifest {
                let mut manifest = #registry_crate::capability::PluginManifest::new(#id);
                #(
                    manifest.dependencies.push(#deps.to_string());
                )*
                manifest
            }

            fn install(
                &self,
                registry: &mut #runtime_crate::plugins::PluginInstallContext<'_>,
            ) -> #runtime_crate::plugins::PluginResult<()> {
                self.install(registry)
            }
        }
    };

    expanded.into()
}

fn node_struct_ident(fn_ident: &syn::Ident) -> syn::Ident {
    let mut out = String::new();
    let mut capitalize = true;
    for ch in fn_ident.to_string().chars() {
        if ch == '_' {
            capitalize = true;
            continue;
        }
        if capitalize {
            out.extend(ch.to_uppercase());
            capitalize = false;
        } else {
            out.push(ch);
        }
    }
    if out.is_empty() || !out.ends_with("Node") {
        out.push_str("Node");
    }
    syn::Ident::new(&out, fn_ident.span())
}

fn snake_case(raw: &str) -> String {
    let mut out = String::new();
    for (idx, ch) in raw.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if idx != 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}
