//! Writing a parameter update on a host, with the checks the chip will apply.

use alloc::vec;
use alloc::vec::Vec;

use crate::chip::{ParamError, ParamSpec, ParamUpdate, Scalar};

/// The message that sets parameter `name` to `value`. `names` and `specs` are the generated
/// module's `PARAM_NAMES` and `PARAMS`.
pub fn encode_update(
    names: &[&str],
    specs: &[ParamSpec],
    name: &str,
    value: Scalar,
) -> Result<Vec<u8>, ParamError> {
    let at = names
        .iter()
        .position(|known| *known == name)
        .ok_or(ParamError::Id)?;
    let spec = specs.get(at).ok_or(ParamError::Id)?;
    let update = ParamUpdate {
        id: u16::try_from(at).map_err(|_| ParamError::Id)?,
        value: spec.check(value)?,
    };
    let mut buf = vec![0; ParamUpdate::MAX_LEN];
    let len = update.to_slice(&mut buf)?;
    buf.truncate(len);
    Ok(buf)
}
