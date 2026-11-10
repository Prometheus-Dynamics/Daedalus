//! Little-endian fields, length-prefixed strings and bytes, and a cursor that refuses to read
//! past the end of its input.

use crate::WireError;

/// Builds one message body.
#[derive(Default)]
pub struct Writer {
    pub buf: Vec<u8>,
}

impl Writer {
    pub fn new() -> Writer {
        Writer::default()
    }

    pub fn u8(&mut self, value: u8) {
        self.buf.push(value);
    }

    pub fn bool(&mut self, value: bool) {
        self.u8(u8::from(value));
    }

    pub fn u16(&mut self, value: u16) {
        self.buf.extend_from_slice(&value.to_le_bytes());
    }

    pub fn u32(&mut self, value: u32) {
        self.buf.extend_from_slice(&value.to_le_bytes());
    }

    pub fn u64(&mut self, value: u64) {
        self.buf.extend_from_slice(&value.to_le_bytes());
    }

    pub fn i8(&mut self, value: i8) {
        self.u8(value as u8);
    }

    pub fn i16(&mut self, value: i16) {
        self.u16(value as u16);
    }

    pub fn i32(&mut self, value: i32) {
        self.u32(value as u32);
    }

    pub fn i64(&mut self, value: i64) {
        self.u64(value as u64);
    }

    pub fn f32(&mut self, value: f32) {
        self.u32(value.to_bits());
    }

    pub fn f64(&mut self, value: f64) {
        self.u64(value.to_bits());
    }

    /// A count or length, as a u32.
    pub fn len(&mut self, len: usize) -> Result<(), WireError> {
        let len32 = u32::try_from(len).map_err(|_| WireError::TooLong(len))?;
        self.u32(len32);
        Ok(())
    }

    pub fn bytes(&mut self, bytes: &[u8]) -> Result<(), WireError> {
        self.len(bytes.len())?;
        self.buf.extend_from_slice(bytes);
        Ok(())
    }

    pub fn str(&mut self, text: &str) -> Result<(), WireError> {
        self.bytes(text.as_bytes())
    }
}

/// Reads one message body, front to back.
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Reader<'a> {
        Reader { buf, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub fn take(&mut self, count: usize) -> Result<&'a [u8], WireError> {
        if count > self.remaining() {
            return Err(WireError::Truncated {
                needed: count,
                remaining: self.remaining(),
            });
        }
        let out = &self.buf[self.pos..self.pos + count];
        self.pos += count;
        Ok(out)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], WireError> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }

    pub fn u8(&mut self) -> Result<u8, WireError> {
        Ok(self.array::<1>()?[0])
    }

    pub fn bool(&mut self) -> Result<bool, WireError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            tag => Err(WireError::BadTag {
                what: "bool",
                tag: u32::from(tag),
            }),
        }
    }

    pub fn u16(&mut self) -> Result<u16, WireError> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    pub fn u32(&mut self) -> Result<u32, WireError> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    pub fn u64(&mut self) -> Result<u64, WireError> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    pub fn i8(&mut self) -> Result<i8, WireError> {
        Ok(self.u8()? as i8)
    }

    pub fn i16(&mut self) -> Result<i16, WireError> {
        Ok(self.u16()? as i16)
    }

    pub fn i32(&mut self) -> Result<i32, WireError> {
        Ok(self.u32()? as i32)
    }

    pub fn i64(&mut self) -> Result<i64, WireError> {
        Ok(self.u64()? as i64)
    }

    pub fn f32(&mut self) -> Result<f32, WireError> {
        Ok(f32::from_bits(self.u32()?))
    }

    pub fn f64(&mut self) -> Result<f64, WireError> {
        Ok(f64::from_bits(self.u64()?))
    }

    /// A length prefix, checked against the bytes left: a count of items that each take at least
    /// one byte cannot be larger than that.
    pub fn count(&mut self) -> Result<usize, WireError> {
        let count = self.u32()?;
        if count as usize > self.remaining() {
            return Err(WireError::CountTooLarge {
                count,
                remaining: self.remaining(),
            });
        }
        Ok(count as usize)
    }

    pub fn bytes(&mut self) -> Result<Vec<u8>, WireError> {
        let len = self.count()?;
        Ok(self.take(len)?.to_vec())
    }

    pub fn str(&mut self) -> Result<String, WireError> {
        let len = self.count()?;
        let raw = self.take(len)?;
        core::str::from_utf8(raw)
            .map(str::to_owned)
            .map_err(|_| WireError::InvalidUtf8)
    }

    /// Refuses bytes left after a complete message.
    pub fn finish(&self) -> Result<(), WireError> {
        match self.remaining() {
            0 => Ok(()),
            left => Err(WireError::TrailingBytes(left)),
        }
    }
}
