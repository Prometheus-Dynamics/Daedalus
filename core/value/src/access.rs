//! How a consumer intends to use an input.

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Access {
    /// Shared and immutable.
    #[default]
    Read,
    /// Takes ownership.
    Move,
    /// Mutates in place.
    Modify,
    /// A borrowed reinterpretation.
    View,
}

impl Access {
    /// Whether a value offered with this access can serve a consumer that requires `required`.
    pub const fn satisfies(self, required: Access) -> bool {
        match required {
            Access::Read => true,
            Access::View => matches!(self, Access::View),
            Access::Move => matches!(self, Access::Move | Access::Modify),
            Access::Modify => matches!(self, Access::Modify),
        }
    }
}
