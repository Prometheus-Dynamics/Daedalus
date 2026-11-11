//! The postcard wire encoding of parameter updates, without serde: the
//! device decodes with [`Reader`] and the host writes through
//! [`Write`], so both sides share one codec. Unsigned integers are LEB128 varints, signed ones
//! zigzag varints, `u8`/`i8`/`bool` one byte, floats little-endian, sequences a varint length
//! then the elements, enums a varint variant index then the fields.

use super::scalar::Scalar;
use crate::scalar_kind::ScalarKind;

/// Malformed or truncated input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct WireError;

/// A cursor over encoded bytes.
#[derive(Clone, Copy, Debug)]
pub struct Reader<'a> {
    buf: &'a [u8],
}

impl<'a> Reader<'a> {
    pub const fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    /// The bytes not read yet.
    pub const fn rest(&self) -> &'a [u8] {
        self.buf
    }

    pub fn bytes<const N: usize>(&mut self) -> Result<[u8; N], WireError> {
        let (head, rest) = self.buf.split_first_chunk::<N>().ok_or(WireError)?;
        self.buf = rest;
        Ok(*head)
    }

    pub fn u8(&mut self) -> Result<u8, WireError> {
        Ok(self.bytes::<1>()?[0])
    }

    pub fn bool(&mut self) -> Result<bool, WireError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(WireError),
        }
    }

    pub fn varint(&mut self) -> Result<u64, WireError> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let byte = self.u8()?;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return if shift == 63 && byte > 1 {
                    Err(WireError)
                } else {
                    Ok(value)
                };
            }
        }
        Err(WireError)
    }

    pub fn u16(&mut self) -> Result<u16, WireError> {
        self.varint()?.try_into().map_err(|_| WireError)
    }

    pub fn u32(&mut self) -> Result<u32, WireError> {
        self.varint()?.try_into().map_err(|_| WireError)
    }

    pub fn zigzag(&mut self) -> Result<i64, WireError> {
        let v = self.varint()?;
        Ok((v >> 1) as i64 ^ -((v & 1) as i64))
    }

    /// A sequence length, bounded by the bytes left (each element takes at least one).
    pub fn seq_len(&mut self) -> Result<usize, WireError> {
        let len = self.varint()?;
        if len > self.buf.len() as u64 {
            return Err(WireError);
        }
        Ok(len as usize)
    }

    /// A [`Scalar`]: its [`ScalarKind`] tag, then the value.
    pub fn scalar(&mut self) -> Result<Scalar, WireError> {
        let kind = ScalarKind::from_u8(self.u8()?).ok_or(WireError)?;
        let int = |r: &mut Self| r.zigzag();
        Ok(match kind {
            ScalarKind::Bool => Scalar::Bool(self.bool()?),
            ScalarKind::I8 => Scalar::I8(self.u8()? as i8),
            ScalarKind::U8 => Scalar::U8(self.u8()?),
            ScalarKind::I16 => Scalar::I16(int(self)?.try_into().map_err(|_| WireError)?),
            ScalarKind::I32 => Scalar::I32(int(self)?.try_into().map_err(|_| WireError)?),
            ScalarKind::I64 => Scalar::I64(int(self)?),
            ScalarKind::U16 => Scalar::U16(self.u16()?),
            ScalarKind::U32 => Scalar::U32(self.u32()?),
            ScalarKind::U64 => Scalar::U64(self.varint()?),
            ScalarKind::F32 => Scalar::F32(f32::from_le_bytes(self.bytes()?)),
            ScalarKind::F64 => Scalar::F64(f64::from_le_bytes(self.bytes()?)),
        })
    }

    /// Fail unless every byte was read.
    pub fn finish(&self) -> Result<(), WireError> {
        if self.buf.is_empty() {
            Ok(())
        } else {
            Err(WireError)
        }
    }
}

/// An output for the encoder: [`SliceWriter`] on the device, a `Vec` wrapper on the host.
pub trait Write {
    fn put(&mut self, bytes: &[u8]);

    fn varint(&mut self, mut value: u64) {
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            if value == 0 {
                self.put(&[byte]);
                return;
            }
            self.put(&[byte | 0x80]);
        }
    }

    fn zigzag(&mut self, value: i64) {
        self.varint(((value << 1) ^ (value >> 63)) as u64);
    }

    fn scalar(&mut self, value: Scalar) {
        self.put(&[value.kind() as u8]);
        match value {
            Scalar::Bool(v) => self.put(&[v.into()]),
            Scalar::I8(v) => self.put(&v.to_le_bytes()),
            Scalar::U8(v) => self.put(&[v]),
            Scalar::I16(v) => self.zigzag(v.into()),
            Scalar::I32(v) => self.zigzag(v.into()),
            Scalar::I64(v) => self.zigzag(v),
            Scalar::U16(v) => self.varint(v.into()),
            Scalar::U32(v) => self.varint(v.into()),
            Scalar::U64(v) => self.varint(v),
            Scalar::F32(v) => self.put(&v.to_le_bytes()),
            Scalar::F64(v) => self.put(&v.to_le_bytes()),
        }
    }
}

/// Writes into a byte slice; [`SliceWriter::finish`] fails if it did not fit.
pub struct SliceWriter<'a> {
    buf: &'a mut [u8],
    len: usize,
    overflow: bool,
}

impl<'a> SliceWriter<'a> {
    pub fn new(buf: &'a mut [u8]) -> Self {
        Self {
            buf,
            len: 0,
            overflow: false,
        }
    }

    /// The written length.
    pub fn finish(self) -> Result<usize, WireError> {
        if self.overflow {
            Err(WireError)
        } else {
            Ok(self.len)
        }
    }
}

impl Write for SliceWriter<'_> {
    fn put(&mut self, bytes: &[u8]) {
        match self.buf.get_mut(self.len..self.len + bytes.len()) {
            Some(dst) => {
                dst.copy_from_slice(bytes);
                self.len += bytes.len();
            }
            None => self.overflow = true,
        }
    }
}
