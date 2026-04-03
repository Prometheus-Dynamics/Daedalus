use proc_macro2::TokenStream as TokenStream2;

pub fn map_texture_format(fmt: &str) -> Option<TokenStream2> {
    match fmt.to_ascii_lowercase().as_str() {
        "r8unorm" => Some(quote::quote! { Some(::daedalus::gpu::wgpu::TextureFormat::R8Unorm) }),
        "rgba8unorm" => Some(quote::quote! { Some(::daedalus::gpu::wgpu::TextureFormat::Rgba8Unorm) }),
        "rgba8unorm_srgb" => Some(quote::quote! { Some(::daedalus::gpu::wgpu::TextureFormat::Rgba8UnormSrgb) }),
        "rgba8snorm" => Some(quote::quote! { Some(::daedalus::gpu::wgpu::TextureFormat::Rgba8Snorm) }),
        "rgba16float" => Some(quote::quote! { Some(::daedalus::gpu::wgpu::TextureFormat::Rgba16Float) }),
        "r32float" => Some(quote::quote! { Some(::daedalus::gpu::wgpu::TextureFormat::R32Float) }),
        "rgba32float" => Some(quote::quote! { Some(::daedalus::gpu::wgpu::TextureFormat::Rgba32Float) }),
        "r16float" => Some(quote::quote! { Some(::daedalus::gpu::wgpu::TextureFormat::R16Float) }),
        "rg16float" => Some(quote::quote! { Some(::daedalus::gpu::wgpu::TextureFormat::Rg16Float) }),
        _ => None,
    }
}

pub fn map_sample_type(sample: &str) -> Option<TokenStream2> {
    match sample.to_ascii_lowercase().as_str() {
        "f32" | "vec4<f32>" => {
            Some(quote::quote! { Some(::daedalus::gpu::wgpu::TextureSampleType::Float { filterable: true }) })
        }
        "float" | "filterable-float" => {
            Some(quote::quote! { Some(::daedalus::gpu::wgpu::TextureSampleType::Float { filterable: true }) })
        }
        "unfilterable-float" => {
            Some(quote::quote! { Some(::daedalus::gpu::wgpu::TextureSampleType::Float { filterable: false }) })
        }
        "sint" | "i32" => Some(quote::quote! { Some(::daedalus::gpu::wgpu::TextureSampleType::Sint) }),
        "uint" | "u32" => Some(quote::quote! { Some(::daedalus::gpu::wgpu::TextureSampleType::Uint) }),
        _ => None,
    }
}

pub fn map_view_dimension(view: &str) -> Option<TokenStream2> {
    match view.to_ascii_lowercase().as_str() {
        "2d" => Some(quote::quote! { Some(::daedalus::gpu::wgpu::TextureViewDimension::D2) }),
        "2d_array" | "2d-array" | "2darray" => {
            Some(quote::quote! { Some(::daedalus::gpu::wgpu::TextureViewDimension::D2Array) })
        }
        "cube" | "cubemap" => Some(quote::quote! { Some(::daedalus::gpu::wgpu::TextureViewDimension::Cube) }),
        _ => None,
    }
}

pub fn map_sampler_kind(kind: &str) -> Option<TokenStream2> {
    match kind.to_ascii_lowercase().as_str() {
        "comparison" => Some(quote::quote! { ::daedalus::gpu::shader::SamplerKind::Comparison }),
        "nonfiltering" => {
            Some(quote::quote! { ::daedalus::gpu::shader::SamplerKind::NonFiltering })
        }
        "filtering" | "nearest" | "linear" => {
            Some(quote::quote! { ::daedalus::gpu::shader::SamplerKind::Filtering })
        }
        _ => None,
    }
}

pub fn map_address_mode(mode: &str) -> Option<TokenStream2> {
    match mode.to_ascii_lowercase().as_str() {
        "repeat" => Some(quote::quote! { ::daedalus::gpu::wgpu::AddressMode::Repeat }),
        "mirror" | "mirror_repeat" => Some(quote::quote! { ::daedalus::gpu::wgpu::AddressMode::MirrorRepeat }),
        "clamp" | "clamp_to_edge" => Some(quote::quote! { ::daedalus::gpu::wgpu::AddressMode::ClampToEdge }),
        _ => None,
    }
}

pub fn map_mipmap_filter(mode: &str) -> Option<TokenStream2> {
    match mode.to_ascii_lowercase().as_str() {
        "nearest" => Some(quote::quote! { ::daedalus::gpu::wgpu::MipmapFilterMode::Nearest }),
        "linear" => Some(quote::quote! { ::daedalus::gpu::wgpu::MipmapFilterMode::Linear }),
        _ => None,
    }
}
