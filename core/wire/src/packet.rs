//! Framing: a u32 little-endian length, then the body. The reader checks the length against the
//! cap before it allocates anything for the body.

use std::io::{ErrorKind, Read, Write};

use crate::PacketError;

/// The default cap: 16 MiB.
pub const DEFAULT_CAP: u32 = 16 * 1024 * 1024;

/// Writes one packet. A body over the cap is refused before anything is written.
pub fn write_packet(out: &mut impl Write, body: &[u8], cap: u32) -> Result<(), PacketError> {
    let len =
        u32::try_from(body.len()).map_err(|_| PacketError::TooLarge { len: u32::MAX, cap })?;
    if len > cap {
        return Err(PacketError::TooLarge { len, cap });
    }
    out.write_all(&len.to_le_bytes()).map_err(PacketError::Io)?;
    out.write_all(body).map_err(PacketError::Io)?;
    out.flush().map_err(PacketError::Io)
}

/// Reads one packet into `buf`, which keeps its capacity between packets.
pub fn read_packet(input: &mut impl Read, cap: u32, buf: &mut Vec<u8>) -> Result<(), PacketError> {
    let mut header = [0u8; 4];
    let mut got = 0;
    while got < header.len() {
        match input.read(&mut header[got..]) {
            Ok(0) if got == 0 => return Err(PacketError::Closed),
            Ok(0) => return Err(PacketError::Truncated),
            Ok(count) => got += count,
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => return Err(PacketError::Io(error)),
        }
    }
    let len = u32::from_le_bytes(header);
    if len > cap {
        return Err(PacketError::TooLarge { len, cap });
    }
    buf.clear();
    buf.resize(len as usize, 0);
    input.read_exact(buf).map_err(|error| match error.kind() {
        ErrorKind::UnexpectedEof => PacketError::Truncated,
        _ => PacketError::Io(error),
    })
}
