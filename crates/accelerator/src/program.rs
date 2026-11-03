use crate::{AcceleratorError, Binding};

/// A program image to load: its format (one of `AcceleratorInfo::program_formats`), the bytes,
/// and format-specific options (for example a core mask or a priority).
#[derive(Clone, Copy, Debug)]
pub struct ProgramDesc<'a> {
    pub format: &'a str,
    pub image: &'a [u8],
    pub options: &'a [(&'a str, &'a str)],
}

impl<'a> ProgramDesc<'a> {
    pub fn new(format: &'a str, image: &'a [u8]) -> Self {
        Self {
            format,
            image,
            options: &[],
        }
    }

    pub fn with_options(mut self, options: &'a [(&'a str, &'a str)]) -> Self {
        self.options = options;
        self
    }
}

/// One input or output of a loaded program.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SlotInfo {
    pub slot: u32,
    pub name: String,
    /// Bytes the slot's buffer must hold.
    pub len: usize,
}

/// A loaded program's slots: inputs are read, outputs written.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProgramInfo {
    pub inputs: Vec<SlotInfo>,
    pub outputs: Vec<SlotInfo>,
}

impl ProgramInfo {
    /// The slot numbered `slot`, and whether it is an output.
    pub fn slot(&self, slot: u32) -> Option<(&SlotInfo, bool)> {
        self.inputs
            .iter()
            .map(|s| (s, false))
            .chain(self.outputs.iter().map(|s| (s, true)))
            .find(|(s, _)| s.slot == slot)
    }
}

/// The checks every accelerator makes on a submission's bindings: each slot bound exactly once,
/// to a buffer large enough, with an access that matches its direction (inputs read, outputs
/// written).
pub fn validate_bindings(info: &ProgramInfo, bindings: &[Binding]) -> Result<(), AcceleratorError> {
    let error = |slot, reason: &str| AcceleratorError::Binding {
        slot,
        reason: reason.into(),
    };
    for (i, binding) in bindings.iter().enumerate() {
        let (slot, output) = info
            .slot(binding.slot)
            .ok_or_else(|| error(binding.slot, "the program has no such slot"))?;
        if bindings[..i].iter().any(|b| b.slot == binding.slot) {
            return Err(error(binding.slot, "bound twice"));
        }
        if binding.buffer.len() < slot.len {
            return Err(error(
                binding.slot,
                &format!(
                    "needs {} bytes, the buffer has {}",
                    slot.len,
                    binding.buffer.len()
                ),
            ));
        }
        let fits = if output {
            binding.access.writes()
        } else {
            binding.access.reads()
        };
        if !fits {
            let need = if output {
                "an output is written"
            } else {
                "an input is read"
            };
            return Err(error(binding.slot, need));
        }
    }
    for slot in info.inputs.iter().chain(&info.outputs) {
        if !bindings.iter().any(|b| b.slot == slot.slot) {
            return Err(error(slot.slot, "not bound"));
        }
    }
    Ok(())
}
