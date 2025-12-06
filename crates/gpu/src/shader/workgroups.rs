use super::BindingKind;
use super::prepare::Prepared;
use crate::GpuError;

pub(crate) fn derive_workgroups(
    prepared: &[Prepared],
    wg_x: u32,
    wg_y: u32,
    wg_z: u32,
) -> Result<[u32; 3], GpuError> {
    // Fallback: derive invocation grid from textures (max width/height) or storage buffers with an explicit invocation_stride.
    let mut tex_dims: Option<(u32, u32)> = None;
    let mut inv_opt: Option<u32> = None;
    let mut missing_stride = false;
    for p in prepared {
        if let Prepared::Texture { width, height, .. } = p {
            tex_dims = match tex_dims {
                Some((max_w, max_h)) => Some((max_w.max(*width), max_h.max(*height))),
                None => Some((*width, *height)),
            };
        }
        if let Prepared::Buffer { spec, size, .. } = p
            && matches!(spec.kind, BindingKind::Storage)
        {
            match spec.invocation_stride {
                Some(stride) if stride > 0 => {
                    let stride_u64 = stride as u64;
                    if size % stride_u64 != 0 {
                        return Err(GpuError::Internal(format!(
                            "storage buffer binding {} size {} not divisible by invocation_stride {}",
                            spec.binding, size, stride
                        )));
                    }
                    let elems = (size / stride_u64).min(u32::MAX as u64) as u32;
                    inv_opt = Some(inv_opt.unwrap_or(0).max(elems));
                }
                _ => {
                    missing_stride = true;
                }
            }
        }
    }
    if let Some((w, h)) = tex_dims {
        let x = w.div_ceil(wg_x);
        let y = h.div_ceil(wg_y);
        return Ok([x.max(1), y.max(1), wg_z]);
    }
    if let Some(inv) = inv_opt {
        let x = inv.div_ceil(wg_x);
        return Ok([x.max(1), 1, 1]);
    }
    if missing_stride {
        return Err(GpuError::Internal(
            "workgroup count not provided; storage buffer stride unknown. Provide `invocations`/`workgroups` or set `invocation_stride`."
                .into(),
        ));
    }
    Err(GpuError::Internal(
        "workgroup count not provided and could not be inferred".into(),
    ))
}
