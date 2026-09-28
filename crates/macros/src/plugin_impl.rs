use proc_macro::TokenStream;
use quote::quote;
use syn::{Expr, ItemStruct, Lit, LitStr, Meta, MetaList, MetaNameValue, Path, parse_macro_input};

use crate::helpers::{
    AttributeArgs, NestedMeta, compile_error, crate_path, lit_from_expr, parse_nested,
};

struct PluginArgs {
    id: LitStr,
    deps: Vec<LitStr>,
    types: Vec<Path>,
    values: Vec<Path>,
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

fn collect_path_list(list: &MetaList, what: &str) -> Result<Vec<Path>, proc_macro2::TokenStream> {
    parse_nested(list)?
        .into_iter()
        .map(|item| match item {
            NestedMeta::Meta(Meta::Path(path)) => Ok(path),
            _ => Err(compile_error(format!(
                "plugin {what} entries must be paths"
            ))),
        })
        .collect()
}

fn parse_args(args: AttributeArgs) -> Result<PluginArgs, proc_macro2::TokenStream> {
    let mut id = None;
    let mut deps = Vec::new();
    let mut types = Vec::new();
    let mut values = Vec::new();
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
                types = collect_path_list(&list, "types")?;
            }
            NestedMeta::Meta(Meta::List(list)) if list.path.is_ident("values") => {
                values = collect_path_list(&list, "values")?;
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
                parts = collect_path_list(&list, "parts")?;
            }
            _ => {
                return Err(compile_error(
                    "plugin arguments must use `id = \"...\", install = setup, deps(...), parts(...), types(...), values(...), nodes(...), adapters(...), devices(...)`"
                        .into(),
                ));
            }
        }
    }

    Ok(PluginArgs {
        id: id.ok_or_else(|| compile_error("missing plugin id".into()))?,
        deps,
        types,
        values,
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
    let runtime_crate = crate_path("daedalus-runtime", "runtime");
    let registry_crate = crate_path("daedalus-registry", "registry");
    let data_crate = crate_path("daedalus-data", "data");

    let ident = input.ident;
    let vis = input.vis;
    let id = parsed.id;
    let deps = parsed.deps;
    let types = parsed.types;
    let values = parsed.values;
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
                    registry.register_daedalus_type::<#types>(
                        #data_crate::named_types::HostExportPolicy::None,
                    )?;
                )*
                #(
                    registry.register_daedalus_value::<#values>()?;
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
                let mut manifest = #registry_crate::capability::PluginManifest::new(#id)
                    .version(env!("CARGO_PKG_VERSION"));
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
