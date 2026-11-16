//! Which adapters a process may open: `DAEDALUS_GPU_ADAPTERS`.
//!
//! `software` keeps only the CPU adapters (software rasterisers such as lavapipe), `hardware`
//! keeps the others, and `any` keeps all of them. Unset means `any`. Every adapter enumeration in
//! this repository filters through [`filter`], whatever API the adapter is reached through, so a
//! hardware adapter can't slip past `software` by way of GL or EGL. Only [`Policy::from_env`] reads
//! the variable.
//!
//! The predicate is the adapter's device type, not its name: a driver that calls its software
//! rasteriser something else is still a CPU device.

use std::fmt;

/// The environment variable that holds the policy.
pub const VARIABLE: &str = "DAEDALUS_GPU_ADAPTERS";

/// Which adapters an enumeration keeps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Policy {
    /// Only CPU adapters.
    Software,
    /// Every adapter that isn't a CPU one.
    Hardware,
    /// Every adapter.
    Any,
}

/// A value of [`VARIABLE`] that is not one of the accepted names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnknownPolicy {
    pub value: String,
}

impl Policy {
    /// Reads the policy from `value`, the text of the variable. Surrounding spaces are ignored;
    /// nothing else is, so `Software` is refused rather than read as `software`.
    pub fn parse(value: &str) -> Result<Policy, UnknownPolicy> {
        match value.trim() {
            "software" => Ok(Policy::Software),
            "hardware" => Ok(Policy::Hardware),
            "any" => Ok(Policy::Any),
            _ => Err(UnknownPolicy {
                value: value.to_owned(),
            }),
        }
    }

    /// The policy in the environment. Unset is [`Policy::Any`]; a value that isn't one of the
    /// names is an error, never a fallback.
    pub fn from_env() -> Result<Policy, UnknownPolicy> {
        match std::env::var(VARIABLE) {
            Ok(value) => Policy::parse(&value),
            Err(std::env::VarError::NotPresent) => Ok(Policy::Any),
            Err(std::env::VarError::NotUnicode(value)) => Err(UnknownPolicy {
                value: value.to_string_lossy().into_owned(),
            }),
        }
    }

    /// The name this policy is written as.
    pub fn name(self) -> &'static str {
        match self {
            Policy::Software => "software",
            Policy::Hardware => "hardware",
            Policy::Any => "any",
        }
    }

    /// Whether an adapter of this kind is visible under the policy.
    pub fn keeps(self, software: bool) -> bool {
        match self {
            Policy::Software => software,
            Policy::Hardware => !software,
            Policy::Any => true,
        }
    }
}

impl fmt::Display for UnknownPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{VARIABLE} is \"{}\", which is not a value it takes: use software, hardware or any",
            self.value
        )
    }
}

impl std::error::Error for UnknownPolicy {}

/// The adapters `policy` keeps, in their order, and how many it hid. `software` says whether an
/// adapter is a CPU one.
pub fn filter<T>(
    policy: Policy,
    adapters: Vec<T>,
    software: impl Fn(&T) -> bool,
) -> (Vec<T>, usize) {
    let total = adapters.len();
    let kept: Vec<T> = adapters
        .into_iter()
        .filter(|adapter| policy.keeps(software(adapter)))
        .collect();
    let hidden = total - kept.len();
    (kept, hidden)
}

#[cfg(test)]
mod tests {
    use super::{Policy, UnknownPolicy, filter};

    /// Fake adapters: a name and whether it is a CPU device.
    fn fake() -> Vec<(&'static str, bool)> {
        vec![
            ("NVIDIA GeForce", false),
            ("llvmpipe (LLVM 19)", true),
            ("Intel Iris", false),
            ("SwiftShader", true),
        ]
    }

    fn names(policy: Policy) -> (Vec<&'static str>, usize) {
        let (kept, hidden) = filter(policy, fake(), |(_, cpu)| *cpu);
        (kept.into_iter().map(|(name, _)| name).collect(), hidden)
    }

    #[test]
    fn software_keeps_only_cpu_adapters() {
        assert_eq!(
            names(Policy::Software),
            (vec!["llvmpipe (LLVM 19)", "SwiftShader"], 2)
        );
    }

    #[test]
    fn hardware_keeps_every_other_adapter() {
        assert_eq!(
            names(Policy::Hardware),
            (vec!["NVIDIA GeForce", "Intel Iris"], 2)
        );
    }

    #[test]
    fn any_keeps_all_and_hides_none() {
        assert_eq!(
            names(Policy::Any),
            (fake().into_iter().map(|(n, _)| n).collect(), 0)
        );
    }

    #[test]
    fn a_cpu_name_on_a_hardware_device_is_still_hardware() {
        // The device type decides, not the name.
        let (kept, hidden) = filter(
            Policy::Software,
            vec![("lavapipe-like", false)],
            |(_, cpu)| *cpu,
        );
        assert!(kept.is_empty());
        assert_eq!(hidden, 1);
    }

    #[test]
    fn nothing_is_hidden_from_an_empty_list() {
        let (kept, hidden) = filter(Policy::Software, Vec::<(&str, bool)>::new(), |(_, cpu)| {
            *cpu
        });
        assert!(kept.is_empty());
        assert_eq!(hidden, 0);
    }

    #[test]
    fn the_accepted_names_parse_and_others_are_refused() {
        assert_eq!(Policy::parse("software"), Ok(Policy::Software));
        assert_eq!(Policy::parse(" hardware "), Ok(Policy::Hardware));
        assert_eq!(Policy::parse("any"), Ok(Policy::Any));
        for bad in ["", "Software", "cpu", "gpu", "all", "software,hardware"] {
            assert_eq!(
                Policy::parse(bad),
                Err(UnknownPolicy {
                    value: bad.to_owned()
                }),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn an_unknown_value_names_the_variable_and_the_accepted_values() {
        let text = Policy::parse("gpu").unwrap_err().to_string();
        assert!(text.contains("DAEDALUS_GPU_ADAPTERS"), "{text}");
        assert!(text.contains("software, hardware or any"), "{text}");
    }
}
