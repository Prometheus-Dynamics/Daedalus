pub use daedalus_wgsl_infer::{InferredAccess, InferredBinding};
use proc_macro2::Span;

#[derive(Clone)]
pub struct Spec {
    pub src: String,
    pub entry: String,
    pub workgroup: Option<[u32; 3]>,
}

pub enum FieldKind {
    StorageRead,
    StorageReadWrite {
        zeroed: bool,
        readback: bool,
    },
    StorageWrite {
        zeroed: bool,
        readback: bool,
    },
    Uniform,
    State,
    Texture {
        source: TextureSource,
        write: bool,
        format_override: Option<String>,
        sample_type_override: Option<String>,
        view_override: Option<String>,
    },
    Sampler {
        kind_override: Option<String>,
        address_override: Option<String>,
        mipmap_override: Option<String>,
    },
}

pub struct ParsedField {
    pub ident: syn::Ident,
    pub binding: Option<u32>,
    pub storage_flag: bool,
    pub storage_rw: bool,
    pub uniform_flag: bool,
    pub zeroed: bool,
    pub readback: bool,
    pub texture_flag: bool,
    pub state_flag: bool,
    pub sampler_flag: bool,
    pub texture_source: Option<TextureSource>,
    pub tex_format: Option<String>,
    pub tex_sample_type: Option<String>,
    pub tex_view: Option<String>,
    pub sampler_kind: Option<String>,
    pub sampler_address: Option<String>,
    pub sampler_mipmap: Option<String>,
    pub is_state: bool,
}

pub struct FieldBinding {
    pub ident: syn::Ident,
    pub binding: u32,
    pub kind: FieldKind,
    pub invocation_hint: Option<proc_macro2::TokenStream>,
}

#[derive(Clone)]
pub enum TextureSource {
    ComputeDynamic,
    DynamicImage,
    GpuHandle,
    RefGpuHandle,
    TextureOut,
}

pub fn detect_texture_source(ty: &syn::Type) -> syn::Result<Option<TextureSource>> {
    use syn::{Type, TypePath};
    let mut inner = ty;
    let mut is_ref = false;
    if let Type::Reference(r) = inner {
        is_ref = true;
        inner = &*r.elem;
    }
    if let Type::Path(TypePath { path, .. }) = inner {
        let last = path.segments.last().ok_or_else(|| {
            syn::Error::new(Span::call_site(), "texture binding type not recognized")
        })?;
        let ident = last.ident.to_string();
        if ident == "TextureOut" {
            return Ok(Some(TextureSource::TextureOut));
        }
        if ident == "GpuImageHandle" {
            return Ok(Some(if is_ref {
                TextureSource::RefGpuHandle
            } else {
                TextureSource::GpuHandle
            }));
        }
        if ident == "DynamicImage" {
            return Ok(Some(TextureSource::DynamicImage));
        }
        if ident == "Compute"
            && let syn::PathArguments::AngleBracketed(ab) = &last.arguments
            && let Some(syn::GenericArgument::Type(Type::Path(tp))) = ab.args.first()
            && tp
                .path
                .segments
                .last()
                .map(|s| s.ident == "DynamicImage")
                .unwrap_or(false)
        {
            return Ok(Some(TextureSource::ComputeDynamic));
        }
    }
    Err(syn::Error::new(
        Span::call_site(),
        "texture2d binding expects Compute<DynamicImage>, DynamicImage, or GpuImageHandle",
    ))
}
