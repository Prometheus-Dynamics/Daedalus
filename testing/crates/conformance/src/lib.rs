//! The checks every backend must pass, and a macro that makes them tests for one backend.
//!
//! This crate installs the counting allocator as the global allocator, so link it only into test
//! binaries.

#![forbid(unsafe_code)]

mod checks;
mod scenario;

use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};

use daedalus_abi::counting::CountingAlloc;
use daedalus_backend::Backend;

pub use scenario::Scenario;

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

/// One check against one backend.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckResult {
    pub check: &'static str,
    pub backend: String,
    /// The nodes the check used.
    pub region: Vec<u32>,
    /// `None` when the check passed.
    pub failure: Option<String>,
}

impl CheckResult {
    pub fn passed(&self) -> bool {
        self.failure.is_none()
    }
}

impl fmt::Display for CheckResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let outcome = self.failure.as_deref().unwrap_or("passed");
        write!(
            f,
            "{} on backend `{}`, region {:?}: {outcome}",
            self.check, self.backend, self.region
        )
    }
}

fn run<B: Backend>(
    check: &'static str,
    body: checks::Check<B>,
    make: &dyn Fn() -> B,
    scenario: &Scenario,
) -> CheckResult {
    let backend = make();
    let taken = catch_unwind(AssertUnwindSafe(|| scenario.taken(&backend)));
    let region = taken
        .unwrap_or_default()
        .iter()
        .map(|node| node.node.0)
        .collect();
    let outcome = catch_unwind(AssertUnwindSafe(|| body(make, scenario)));
    CheckResult {
        check,
        backend: backend.id().to_string(),
        region,
        failure: match outcome {
            Ok(Ok(())) => None,
            Ok(Err(failure)) => Some(failure),
            Err(_) => Some("the check panicked".to_string()),
        },
    }
}

/// Every check, on the default scenario.
pub fn run_all<B: Backend>(make: impl Fn() -> B) -> Vec<CheckResult> {
    run_all_with(make, &Scenario::default())
}

/// Every check, for a backend whose bodies need a real payload.
pub fn run_all_with<B: Backend>(make: impl Fn() -> B, scenario: &Scenario) -> Vec<CheckResult> {
    checks::all::<B>()
        .into_iter()
        .map(|(name, body)| run(name, body, &make, scenario))
        .collect()
}

/// One check by name.
pub fn check<B: Backend>(
    name: &str,
    make: impl Fn() -> B,
    scenario: &Scenario,
) -> Option<CheckResult> {
    let (name, body) = checks::all::<B>()
        .into_iter()
        .find(|(known, _)| *known == name)?;
    Some(run(name, body, &make, scenario))
}

/// Emits one `#[test]` per check, so a test name shows what failed. Takes the expression that
/// builds the backend, and optionally a [`Scenario`].
#[macro_export]
macro_rules! conformance {
    ($make:expr) => {
        $crate::conformance!($make, $crate::Scenario::default());
    };
    ($make:expr, $scenario:expr) => {
        $crate::conformance!(@tests $make, $scenario;
            id_is_stable
            memory_ids_are_unique
            transfers_name_known_spaces
            can_take_is_pure
            can_take_returns_only_offered_nodes
            accepted_region_compiles
            refusal_names_only_region_nodes
            covers_stay_inside_the_region
            steps_do_not_allocate
            faults_stay_inside_the_region
            running_is_deterministic
        );
    };
    (@tests $make:expr, $scenario:expr; $($name:ident)*) => {$(
        #[test]
        fn $name() {
            let result = $crate::check(stringify!($name), || $make, &$scenario)
                .expect("a known check");
            assert!(result.passed(), "{result}");
        }
    )*};
}
