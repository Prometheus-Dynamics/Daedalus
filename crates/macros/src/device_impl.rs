use proc_macro::TokenStream;
use quote::quote;
use syn::{FnArg, ItemFn, LitStr, Meta, MetaNameValue, Path, Type, parse_macro_input};

use crate::helpers::{
    AttributeArgs, DaedalusCrate, NestedMeta, compile_error, fn_path_arg, lit_str_arg,
    result_ok_type,
};

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
                id = Some(lit_str_arg(&value, "device id")?);
            }
            NestedMeta::Meta(Meta::NameValue(MetaNameValue { path, value, .. }))
                if path.is_ident("cpu") =>
            {
                cpu = Some(lit_str_arg(&value, "device cpu")?);
            }
            NestedMeta::Meta(Meta::NameValue(MetaNameValue { path, value, .. }))
                if path.is_ident("device") =>
            {
                device = Some(lit_str_arg(&value, "device target")?);
            }
            NestedMeta::Meta(Meta::NameValue(MetaNameValue { path, value, .. }))
                if path.is_ident("download") =>
            {
                download = Some(fn_path_arg(value, "device download")?);
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
    let runtime_crate = DaedalusCrate::Runtime.path();
    let data_crate = DaedalusCrate::Data.path();

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
