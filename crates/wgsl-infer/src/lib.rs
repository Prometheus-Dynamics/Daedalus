/// Inferred access kind for a WGSL binding.
///
/// ```
/// use daedalus_wgsl_infer::InferredAccess;
/// let access = InferredAccess::StorageRead;
/// assert!(matches!(access, InferredAccess::StorageRead));
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InferredAccess {
    StorageRead,
    StorageReadWrite,
    StorageWrite,
    Uniform,
    StorageTexture {
        format: Option<String>,
        view: Option<String>,
    },
    Texture {
        format: Option<String>,
        sample_type: Option<String>,
        view: Option<String>,
    },
    Sampler(Option<String>),
}

/// Inferred binding metadata.
///
/// ```
/// use daedalus_wgsl_infer::{InferredAccess, InferredBinding};
/// let binding = InferredBinding { binding: 0, access: InferredAccess::Uniform };
/// assert_eq!(binding.binding, 0);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InferredBinding {
    pub binding: u32,
    pub access: InferredAccess,
}

/// Inferred workgroup and bindings for a WGSL entry point.
///
/// ```
/// use daedalus_wgsl_infer::InferredSpec;
/// let spec = InferredSpec { workgroup: None, bindings: vec![] };
/// assert!(spec.bindings.is_empty());
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InferredSpec {
    pub workgroup: Option<[u32; 3]>,
    pub bindings: Vec<InferredBinding>,
}

/// Infer the `@workgroup_size` annotation from WGSL source.
///
/// ```
/// use daedalus_wgsl_infer::infer_workgroup_size;
/// let wgsl = "@compute @workgroup_size(8, 4, 1) fn main() {}";
/// assert_eq!(infer_workgroup_size(wgsl), Some([8, 4, 1]));
/// ```
pub fn infer_workgroup_size(src: &str) -> Option<[u32; 3]> {
    if let Some(idx) = src.find("@workgroup_size") {
        let rest = &src[idx..];
        if let Some(start) = rest.find('(') {
            let rest = &rest[start + 1..];
            let mut parts = rest.split(|c| [',', ')'].contains(&c));
            let x = parts
                .next()
                .and_then(|s| s.trim().parse::<u32>().ok())
                .unwrap_or(0);
            let y = parts
                .next()
                .and_then(|s| s.trim().parse::<u32>().ok())
                .unwrap_or(1);
            let z = parts
                .next()
                .and_then(|s| s.trim().parse::<u32>().ok())
                .unwrap_or(1);
            if x > 0 {
                return Some([x, y.max(1), z.max(1)]);
            }
        }
    }
    None
}

/// Infer bindings from WGSL source.
///
/// ```
/// use daedalus_wgsl_infer::infer_bindings;
/// let wgsl = "@group(0) @binding(0) var<uniform> Params: vec4<f32>;";
/// let bindings = infer_bindings(wgsl);
/// assert_eq!(bindings.len(), 1);
/// ```
pub fn infer_bindings(src: &str) -> Vec<InferredBinding> {
    let mut bindings = Vec::new();
    let mut offset = 0;
    while let Some(rel_idx) = src[offset..].find("@binding") {
        let idx = offset + rel_idx;
        let rest = &src[idx..];
        let decl = if let Some(end) = rest.find(';') {
            &rest[..=end]
        } else {
            rest
        };
        let num = rest
            .split(|c: char| !c.is_ascii_digit())
            .find_map(|chunk| chunk.parse::<u32>().ok());
        let Some(binding) = num else {
            offset = idx + "@binding".len();
            continue;
        };
        let lower = decl.to_ascii_lowercase();
        let mut format = None;
        let mut sample_type = None;
        let access = if lower.contains("var<storage") {
            if lower.contains("read_write") {
                InferredAccess::StorageReadWrite
            } else if lower.contains("write") && !lower.contains("read") {
                InferredAccess::StorageWrite
            } else {
                InferredAccess::StorageRead
            }
        } else if lower.contains("var<uniform") {
            InferredAccess::Uniform
        } else if lower.contains("texture_storage_2d") {
            let view = Some("2d".to_string());
            if let Some(lt) = lower.find('<')
                && let Some(gt) = lower[lt..].find('>')
            {
                let inner = &lower[lt + 1..lt + gt];
                let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
                if let Some(fmt) = parts.first() {
                    format = Some(fmt.to_string());
                }
            }
            InferredAccess::StorageTexture { format, view }
        } else if lower.contains("texture_2d_array") {
            let view = Some("2d_array".to_string());
            if let Some(lt) = lower.find('<')
                && let Some(gt) = lower[lt..].find('>')
            {
                let inner = &lower[lt + 1..lt + gt];
                sample_type = Some(inner.trim().to_string());
            }
            InferredAccess::Texture {
                format: None,
                sample_type,
                view,
            }
        } else if lower.contains("texture_2d") {
            let view = Some("2d".to_string());
            if let Some(lt) = lower.find('<')
                && let Some(gt) = lower[lt..].find('>')
            {
                let inner = &lower[lt + 1..lt + gt];
                sample_type = Some(inner.trim().to_string());
            }
            InferredAccess::Texture {
                format: None,
                sample_type,
                view,
            }
        } else if lower.contains("sampler_comparison") {
            InferredAccess::Sampler(Some("comparison".into()))
        } else if lower.contains("sampler") {
            InferredAccess::Sampler(Some("filtering".into()))
        } else {
            offset = idx + "@binding".len();
            continue;
        };
        bindings.push(InferredBinding { binding, access });
        offset = idx + decl.len();
    }
    bindings
}

/// Infer both workgroup size and bindings from WGSL source.
///
/// ```
/// use daedalus_wgsl_infer::infer_spec;
/// let wgsl = "@compute @workgroup_size(1) fn main() {}";
/// let spec = infer_spec(wgsl);
/// assert!(spec.workgroup.is_some());
/// ```
pub fn infer_spec(src: &str) -> InferredSpec {
    InferredSpec {
        workgroup: infer_workgroup_size(src),
        bindings: infer_bindings(src),
    }
}
