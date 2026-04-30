use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::Span;
use quote::quote;
use syn::{
    Expr, FnArg, ItemFn, Lit, LitStr, Meta, MetaNameValue, Path, ReturnType, Type,
    parse_macro_input,
};

use crate::helpers::{AttributeArgs, NestedMeta, compile_error, lit_from_expr};

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

struct DeviceArgs {
    id: LitStr,
    cpu: LitStr,
    device: LitStr,
    download: Path,
}

fn parse_args(args: AttributeArgs) -> Result<DeviceArgs, proc_macro2::TokenStream> {
    let mut id = None;
    let mut cpu = None;
    let mut device = None;
    let mut download = None;

    for arg in args {
        match arg {
            NestedMeta::Meta(Meta::NameValue(MetaNameValue { path, value, .. }))
                if path.is_ident("id") =>
            {
                let Some(Lit::Str(value)) = lit_from_expr(&value) else {
                    return Err(compile_error("device id must be a string literal".into()));
                };
                id = Some(value);
            }
            NestedMeta::Meta(Meta::NameValue(MetaNameValue { path, value, .. }))
                if path.is_ident("cpu") =>
            {
                let Some(Lit::Str(value)) = lit_from_expr(&value) else {
                    return Err(compile_error("device cpu must be a string literal".into()));
                };
                cpu = Some(value);
            }
            NestedMeta::Meta(Meta::NameValue(MetaNameValue { path, value, .. }))
                if path.is_ident("device") =>
            {
                let Some(Lit::Str(value)) = lit_from_expr(&value) else {
                    return Err(compile_error(
                        "device target must be a string literal".into(),
                    ));
                };
                device = Some(value);
            }
            NestedMeta::Meta(Meta::NameValue(MetaNameValue { path, value, .. }))
                if path.is_ident("download") =>
            {
                let Expr::Path(path) = value else {
                    return Err(compile_error(
                        "device download must be a function path".into(),
                    ));
                };
                download = Some(path.path);
            }
            _ => {
                return Err(compile_error(
                    "device arguments must use `id = \"...\", cpu = \"...\", device = \"...\", download = download_fn`"
                        .into(),
                ));
            }
        }
    }

    Ok(DeviceArgs {
        id: id.ok_or_else(|| compile_error("missing device id".into()))?,
        cpu: cpu.ok_or_else(|| compile_error("missing device cpu type key".into()))?,
        device: device.ok_or_else(|| compile_error("missing device target type key".into()))?,
        download: download
            .ok_or_else(|| compile_error("missing device download function".into()))?,
    })
}

fn result_ok_type(output: &ReturnType) -> Option<&Type> {
    let ReturnType::Type(_, ty) = output else {
        return None;
    };
    let Type::Path(path) = ty.as_ref() else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != "Result" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    args.args.first().and_then(|arg| match arg {
        syn::GenericArgument::Type(ty) => Some(ty),
        _ => None,
    })
}

fn borrowed_input_type(ty: &Type) -> Option<&Type> {
    let Type::Reference(reference) = ty else {
        return None;
    };
    if reference.mutability.is_some() {
        return None;
    }
    Some(reference.elem.as_ref())
}

pub fn device(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args with AttributeArgs::parse_terminated);
    let input = parse_macro_input!(item as ItemFn);

    let parsed = match parse_args(args) {
        Ok(parsed) => parsed,
        Err(err) => return err.into(),
    };

    if !input.sig.generics.params.is_empty() {
        return compile_error("device upload functions cannot be generic yet".into()).into();
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
    let data_crate = crate_path(
        "daedalus-data",
        "daedalus_data",
        Some("data"),
        &daedalus_root,
    );

    let fn_ident = &input.sig.ident;
    let register_ident = syn::Ident::new(&format!("register_{fn_ident}_device"), fn_ident.span());
    let vis = &input.vis;
    let id = parsed.id;
    let cpu = parsed.cpu;
    let device = parsed.device;
    let download = parsed.download;
    let upload_id = LitStr::new(&format!("{}.upload", id.value()), id.span());
    let download_id = LitStr::new(&format!("{}.download", id.value()), id.span());

    let mut typed_args = input.sig.inputs.iter().filter_map(|arg| match arg {
        FnArg::Typed(pat) => Some(pat),
        FnArg::Receiver(_) => None,
    });
    let Some(arg) = typed_args.next() else {
        return compile_error(
            "device upload functions must take exactly one borrowed argument".into(),
        )
        .into();
    };
    if typed_args.next().is_some() {
        return compile_error(
            "device upload functions must take exactly one borrowed argument".into(),
        )
        .into();
    }
    let Some(cpu_ty) = borrowed_input_type(arg.ty.as_ref()) else {
        return compile_error("device upload functions must take `&Cpu`".into()).into();
    };
    let Some(device_ty) = result_ok_type(&input.sig.output) else {
        return compile_error(
            "device upload functions must return `Result<Device, daedalus::transport::TransportError>`"
                .into(),
        )
        .into();
    };

    let expanded = quote! {
        #input

        #vis fn #register_ident(
            into: &mut #runtime_crate::plugins::PluginRegistry,
        ) -> #runtime_crate::plugins::PluginResult<()> {
            into.register_typed_device_transport::<#cpu_ty, #device_ty, _, _>(
                #runtime_crate::plugins::TypedDeviceTransport::new(
                    #id,
                    #data_crate::model::TypeExpr::opaque(#cpu),
                    #data_crate::model::TypeExpr::opaque(#device),
                    #upload_id,
                    #download_id,
                ),
                #fn_ident,
                #download,
            )
        }
    };

    expanded.into()
}
