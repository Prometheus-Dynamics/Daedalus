//! CRC-32 of bytes on the device: the device takes the chunks, the host puts them together.

use daedalus_kernels_sys::crc32_combine;

use super::{CRC_CHUNK, Kernel, KernelError, Kernels};
use crate::buffer::Storage;
use crate::fence::Ticket;

/// The CRC-32 byte table (IEEE, reflected), as little-endian words.
fn table() -> [u8; 1024] {
    let mut out = [0; 1024];
    for (byte, word) in out.as_chunks_mut::<4>().0.iter_mut().enumerate() {
        let mut crc = byte as u32;
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xEDB8_8320 & (crc & 1).wrapping_neg());
        }
        *word = crc.to_le_bytes();
    }
    out
}

/// A checksum the device is taking, or has taken.
pub struct Checksum {
    /// The submit, each chunk's CRC, and how many bytes they cover. `None` for no bytes.
    work: Option<(Ticket, Storage, u64)>,
}

impl Checksum {
    /// Whether the device has finished, so [`Checksum::wait`] won't wait.
    pub fn is_done(&self) -> bool {
        self.work
            .as_ref()
            .is_none_or(|(ticket, _, _)| ticket.is_done())
    }

    /// Waits for the chunks and gives the CRC-32 of the whole.
    pub fn wait(self) -> Result<u32, KernelError> {
        let Some((ticket, chunks, n)) = self.work else {
            return Ok(0);
        };
        ticket.wait().map_err(KernelError::Wait)?;
        let mut words = vec![0; chunks.size() as usize];
        chunks
            .read_into(0, &mut words)
            .map_err(KernelError::Storage)?;
        let mut crc = 0;
        for (at, word) in words.as_chunks::<4>().0.iter().enumerate() {
            let len = (n - at as u64 * CRC_CHUNK).min(CRC_CHUNK);
            crc = crc32_combine(crc, u32::from_le_bytes(*word), len);
        }
        Ok(crc)
    }
}

impl Kernels {
    fn table(&self) -> Result<&Storage, KernelError> {
        if let Some(table) = self.table.get() {
            return Ok(table);
        }
        let made = Storage::zeroed(std::sync::Arc::clone(&self.context), 1024)
            .map_err(KernelError::Storage)?;
        made.write(0, &table()).map_err(KernelError::Storage)?;
        Ok(self.table.get_or_init(|| made))
    }

    /// Records the CRC-32 of each 4 KiB chunk of `n` bytes of `src` into `dst`, one word a
    /// chunk, for a caller that combines them itself.
    pub fn encode_crc32_chunks(
        &self,
        encoder: &mut wgpu::CommandEncoder,
        src: &Storage,
        dst: &Storage,
        n: u64,
    ) -> Result<(), KernelError> {
        let table = self.table()?;
        self.encode_with(encoder, Kernel::Crc32Chunks, src, dst, n, Some(table))
    }

    /// Starts the CRC-32 (IEEE) of `n` bytes of `src` and submits. Never waits: the value is
    /// read by [`Checksum::wait`].
    pub fn crc32(&self, src: &Storage, n: u64) -> Result<Checksum, KernelError> {
        if n == 0 {
            return Ok(Checksum { work: None });
        }
        let chunks = Storage::zeroed(
            std::sync::Arc::clone(&self.context),
            Kernel::Crc32Chunks.output_len(n),
        )
        .map_err(KernelError::Storage)?;
        let mut encoder = self.encoder();
        self.encode_crc32_chunks(&mut encoder, src, &chunks, n)?;
        Ok(Checksum {
            work: Some((self.submit(encoder), chunks, n)),
        })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_table_is_the_reference_tables() {
        let table = super::table();
        let words = table.as_chunks::<4>().0;
        // The CRC of one byte, without the complements, is its table entry.
        assert_eq!(u32::from_le_bytes(words[0]), 0);
        assert_eq!(u32::from_le_bytes(words[1]), 0x7707_3096);
        assert_eq!(u32::from_le_bytes(words[255]), 0x2d02_ef8d);
    }
}
