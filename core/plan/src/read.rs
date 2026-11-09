//! Reading a Blob, with every access bounds-checked.

use crate::format::{
    BODIES, EXTRA, HEADER_LEN, MAGIC, NODES, PlanError, SECTION_COUNT, SECTION_NAMES, SLOTS,
    SPACES, STEP_LEN, STEPS, Span, StepKind, StepRecord, TYPES, VERSION, WORD_LEN,
};

fn word(bytes: &[u8], at: usize) -> Option<u32> {
    let end = at.checked_add(WORD_LEN)?;
    Some(u32::from_le_bytes(bytes.get(at..end)?.try_into().ok()?))
}

#[derive(Clone, Copy, Debug, Default)]
struct Section {
    offset: u32,
    count: u32,
}

/// A parsed Blob. Parsing checks every section and step, so reading steps can't fail.
#[derive(Clone, Copy, Debug)]
pub struct PlanView<'a> {
    bytes: &'a [u8],
    hash: u64,
    sections: [Section; SECTION_COUNT],
}

/// The little-endian words of one part of a section.
#[derive(Clone, Debug)]
pub struct Words<'a> {
    bytes: &'a [u8],
}

impl Iterator for Words<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        let value = word(self.bytes, 0)?;
        self.bytes = self.bytes.get(WORD_LEN..)?;
        Some(value)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let left = self.bytes.len() / WORD_LEN;
        (left, Some(left))
    }

    fn nth(&mut self, index: usize) -> Option<u32> {
        self.bytes = self.bytes.get(index.checked_mul(WORD_LEN)?..)?;
        self.next()
    }
}

impl ExactSizeIterator for Words<'_> {}

impl<'a> PlanView<'a> {
    pub fn parse(bytes: &'a [u8]) -> Result<PlanView<'a>, PlanError> {
        if bytes.len() < HEADER_LEN {
            return Err(PlanError::Truncated);
        }
        if &bytes[..MAGIC.len()] != MAGIC {
            return Err(PlanError::BadMagic);
        }
        let format = u16::from_le_bytes([bytes[8], bytes[9]]);
        if format != VERSION {
            return Err(PlanError::UnknownFormat(format));
        }
        let mut hash = [0; 8];
        hash.copy_from_slice(&bytes[16..24]);
        let mut view = PlanView {
            bytes,
            hash: u64::from_le_bytes(hash),
            sections: [Section::default(); SECTION_COUNT],
        };
        for (index, name) in SECTION_NAMES.iter().enumerate() {
            let at = 24 + index * 8;
            let (Some(offset), Some(count)) = (word(bytes, at), word(bytes, at + 4)) else {
                return Err(PlanError::Truncated);
            };
            let section = Section { offset, count };
            let size = if index == STEPS { STEP_LEN } else { WORD_LEN };
            if view.range(section, 0, u64::from(count), size).is_none() {
                return Err(PlanError::Bounds {
                    section: name,
                    at: offset,
                });
            }
            view.sections[index] = section;
        }
        for at in 0..view.sections[STEPS].count {
            view.check_step(at)?;
        }
        Ok(view)
    }

    fn range(&self, section: Section, start: u64, len: u64, size: usize) -> Option<&'a [u8]> {
        if start.checked_add(len)? > u64::from(section.count) {
            return None;
        }
        let begin = u64::from(section.offset).checked_add(start.checked_mul(size as u64)?)?;
        let end = begin.checked_add(len.checked_mul(size as u64)?)?;
        self.bytes
            .get(usize::try_from(begin).ok()?..usize::try_from(end).ok()?)
    }

    fn step_bytes(&self, at: u32) -> Option<&'a [u8]> {
        self.range(self.sections[STEPS], u64::from(at), 1, STEP_LEN)
    }

    fn read_step(bytes: &[u8]) -> Option<StepRecord> {
        let span = |at| {
            Some(Span {
                start: word(bytes, at)?,
                len: word(bytes, at + 4)?,
            })
        };
        Some(StepRecord {
            kind: StepKind::from_byte(*bytes.first()?)?,
            body: word(bytes, 4)?,
            covers: span(8)?,
            reads: span(16)?,
            writes: span(24)?,
        })
    }

    fn check_step(&self, at: u32) -> Result<(), PlanError> {
        let steps = SECTION_NAMES[STEPS];
        let bytes = self
            .step_bytes(at)
            .ok_or(PlanError::Bounds { section: steps, at })?;
        let step = Self::read_step(bytes).ok_or(PlanError::UnknownKind(bytes[0]))?;
        self.span(NODES, step.covers)?;
        self.span(SLOTS, step.reads)?;
        self.span(SLOTS, step.writes)?;
        if step.body >= self.sections[BODIES].count && self.sections[BODIES].count != 0 {
            return Err(PlanError::Bounds {
                section: SECTION_NAMES[BODIES],
                at: step.body,
            });
        }
        Ok(())
    }

    fn span(&self, section: usize, span: Span) -> Result<Words<'a>, PlanError> {
        self.range(
            self.sections[section],
            u64::from(span.start),
            u64::from(span.len),
            WORD_LEN,
        )
        .map(|bytes| Words { bytes })
        .ok_or(PlanError::Bounds {
            section: SECTION_NAMES[section],
            at: span.start,
        })
    }

    fn all(&self, section: usize) -> Words<'a> {
        let span = Span {
            start: 0,
            len: self.sections[section].count,
        };
        self.span(section, span).unwrap_or(Words { bytes: &[] })
    }

    /// The writer's hash of everything after the header.
    pub fn hash(&self) -> u64 {
        self.hash
    }

    pub fn step_count(&self) -> u32 {
        self.sections[STEPS].count
    }

    pub fn steps(&self) -> impl Iterator<Item = StepRecord> + use<'a> {
        let view = *self;
        (0..view.sections[STEPS].count)
            .filter_map(move |at| view.step_bytes(at).and_then(Self::read_step))
    }

    /// The nodes a span covers.
    pub fn node_list(&self, span: Span) -> Result<Words<'a>, PlanError> {
        self.span(NODES, span)
    }

    /// The slots a span reads or writes.
    pub fn slot_list(&self, span: Span) -> Result<Words<'a>, PlanError> {
        self.span(SLOTS, span)
    }

    pub fn spaces(&self) -> Words<'a> {
        self.all(SPACES)
    }

    pub fn types(&self) -> Words<'a> {
        self.all(TYPES)
    }

    pub fn bodies(&self) -> Words<'a> {
        self.all(BODIES)
    }

    /// Words a form keeps for itself. The plan format doesn't read them.
    pub fn extra(&self) -> Words<'a> {
        self.all(EXTRA)
    }
}
