//! What a planning run is told. Nothing here is global.

use std::sync::Arc;

use crate::assign::ASSIGN_BUDGET;
use crate::form::Form;
use crate::solve::rewrite::Rewrite;

/// The FIFO capacity of [`Config`] unless it says otherwise.
pub const DEFAULT_FIFO_CAPACITY: u32 = 4;

#[derive(Clone)]
pub struct Config {
    /// Backend ids, highest priority first. Backends not named follow in the order given.
    pub priority: Vec<String>,
    /// Also check a graph's own port lists against the declaration.
    pub strict_ports: bool,
    /// Run the lints.
    pub lints: bool,
    /// Graph rewrites, run in this order.
    pub rewrites: Vec<Arc<dyn Rewrite>>,
    /// Search steps allowed per coupled component.
    pub assignment_budget: u64,
    pub form: Form,
    /// Let backends merge nodes. Off for finding which node in a merged region fails.
    pub merge: bool,
    /// The capacity of a FIFO edge that declares none, in the forms that lay out their own queues.
    /// A loader of the steps form has its own setting.
    pub fifo_capacity: u32,
    /// Treat every constant as fixed, even one the graph marks as changeable.
    pub freeze_params: bool,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            priority: Vec::new(),
            strict_ports: false,
            lints: false,
            rewrites: Vec::new(),
            assignment_budget: ASSIGN_BUDGET,
            form: Form::default(),
            merge: true,
            fifo_capacity: DEFAULT_FIFO_CAPACITY,
            freeze_params: false,
        }
    }
}
