//! How a body's function is called, for generating code that calls it directly.

/// What a function parameter is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Read,
    Take,
    Modify,
    OptRead,
    OptTake,
    OptModify,
    State,
    ConfigRef,
    ConfigVal,
    Context,
    FanIn,
}

/// How a parameter reads a value in `cpu.wire`, whose cells hold their type's wire form.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireRead {
    /// Its type has no wire form, so it can't read a value from a worker.
    Refused,
    /// The wire form is decoded into the type, a cost the plan lists.
    Decoded,
    /// A list of numbers, viewed where its bytes are. Decoded only when they aren't aligned.
    Viewed,
}

impl WireRead {
    /// For a parameter of a type with a wire form or not, and a view of its items or not.
    pub const fn of(has_wire: bool, viewed: bool) -> WireRead {
        match (has_wire, viewed) {
            (_, true) => WireRead::Viewed,
            (true, false) => WireRead::Decoded,
            (false, false) => WireRead::Refused,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ParamShape {
    pub role: Role,
    /// The input's position, for a role that has one.
    pub at: u16,
    pub port: &'static str,
    /// The value's type, without any reference or `Option`.
    pub ty: fn() -> &'static str,
    pub copy: bool,
    pub wire: WireRead,
}

#[derive(Clone, Copy, Debug)]
pub struct OutputShape {
    /// Returned as an `Option`, and `None` sends nothing.
    pub optional: bool,
    pub ty: fn() -> &'static str,
    pub copy: bool,
}

/// The function behind a body.
#[derive(Clone, Copy, Debug)]
pub struct BodyShape {
    /// `crate_name::module::function`.
    pub path: &'static str,
    pub params: &'static [ParamShape],
    pub outputs: &'static [OutputShape],
    pub fallible: bool,
}
