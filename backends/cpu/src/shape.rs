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

#[derive(Clone, Copy, Debug)]
pub struct ParamShape {
    pub role: Role,
    /// The input's position, for a role that has one.
    pub at: u16,
    pub port: &'static str,
    /// The value's type, without any reference or `Option`.
    pub ty: fn() -> &'static str,
}

#[derive(Clone, Copy, Debug)]
pub struct OutputShape {
    /// Returned as an `Option`, and `None` sends nothing.
    pub optional: bool,
    pub ty: fn() -> &'static str,
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
