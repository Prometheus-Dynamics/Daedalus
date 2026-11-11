//! Base64 decode, which reports: how long the decoded bytes are, and where the input is invalid.

use super::{Kernel, KernelError, Kernels};
use crate::buffer::Storage;
use crate::fence::Ticket;

/// A decode the device is doing, or has done.
pub struct Decoding {
    /// The submit, and what the op reports: the first invalid offset or all ones, then the
    /// length. `None` when there was nothing to decode.
    work: Option<(Ticket, Storage)>,
}

impl Decoding {
    /// Whether the device has finished, so [`Decoding::wait`] won't wait.
    pub fn is_done(&self) -> bool {
        self.work
            .as_ref()
            .is_none_or(|(ticket, _)| ticket.is_done())
    }

    /// Waits for the decode and gives the decoded length, or where the input is invalid, as the
    /// CPU decoder does. What is in the output after invalid input isn't meaningful.
    pub fn wait(self) -> Result<u64, KernelError> {
        let Some((ticket, status)) = self.work else {
            return Ok(0);
        };
        ticket.wait().map_err(KernelError::Wait)?;
        let mut bytes = [0; 8];
        status
            .read_into(0, &mut bytes)
            .map_err(KernelError::Storage)?;
        let [invalid, len] = *bytes.as_chunks::<4>().0 else {
            return Ok(0);
        };
        match u32::from_le_bytes(invalid) {
            u32::MAX => Ok(u64::from(u32::from_le_bytes(len))),
            at => Err(KernelError::InvalidSymbol { at: u64::from(at) }),
        }
    }
}

impl Kernels {
    /// Decodes `n` base64 symbols from `src` into `dst` and submits. `n` is a multiple of 4,
    /// and `dst` holds `n / 4 * 3` bytes rounded up to a word. Never waits: the result is read
    /// by [`Decoding::wait`].
    pub fn base64_decode(
        &self,
        src: &Storage,
        dst: &Storage,
        n: u64,
    ) -> Result<Decoding, KernelError> {
        if n == 0 {
            return Ok(Decoding { work: None });
        }
        if !n.is_multiple_of(4) {
            return Err(KernelError::Size(format!(
                "base64 input of {n} symbols isn't a multiple of 4"
            )));
        }
        // Each decode has its own report, so two in flight don't share one.
        let status = Storage::zeroed(std::sync::Arc::clone(&self.context), 8)
            .map_err(KernelError::Storage)?;
        status
            .write(0, &[0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0])
            .map_err(KernelError::Storage)?;
        let mut encoder = self.encoder();
        self.encode_with(
            (&mut crate::shader::OpenPass::default(), &mut encoder),
            Kernel::Base64Decode,
            src,
            dst,
            n,
            Some(&status),
        )?;
        Ok(Decoding {
            work: Some((self.submit(encoder), status)),
        })
    }
}
