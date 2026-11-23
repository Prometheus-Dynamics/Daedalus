//! This crate's integration tests that need no process of their own, as one test binary:
//! one link instead of one per file. A file that needs a process to itself is a
//! `[[test]]` of its own in Cargo.toml, with the reason beside it. A new file goes in one
//! or the other; the boundary check refuses a test file in neither.

#[path = "b10_control.rs"]
mod b10_control;
#[path = "b11_lint_explain.rs"]
mod b11_lint_explain;
#[path = "b12_hash_cache.rs"]
mod b12_hash_cache;
#[path = "b12_schema_drift.rs"]
mod b12_schema_drift;
#[path = "b13_api_forms.rs"]
mod b13_api_forms;
#[path = "b14_patch_refresh.rs"]
mod b14_patch_refresh;
#[path = "b15_share.rs"]
mod b15_share;
#[path = "b1_expand_defaults.rs"]
mod b1_expand_defaults;
#[path = "b1_group_edge_policy.rs"]
mod b1_group_edge_policy;
#[path = "b2_ports_types.rs"]
mod b2_ports_types;
#[path = "b3_const_inputs.rs"]
mod b3_const_inputs;
#[path = "b4_adapter_routing.rs"]
mod b4_adapter_routing;
#[path = "b4_conversions.rs"]
mod b4_conversions;
#[path = "b5_rewrite.rs"]
mod b5_rewrite;
#[path = "b6_order_rates.rs"]
mod b6_order_rates;
#[path = "b7_assign.rs"]
mod b7_assign;
#[path = "b7_assign_props.rs"]
mod b7_assign_props;
#[path = "b8_refusal.rs"]
mod b8_refusal;
#[path = "b8_regions.rs"]
mod b8_regions;
#[path = "b9_residency.rs"]
mod b9_residency;
#[path = "b9_stitch.rs"]
mod b9_stitch;
#[path = "c10_reload.rs"]
mod c10_reload;
#[path = "c12_fault.rs"]
mod c12_fault;
#[path = "c13_fault_detail.rs"]
mod c13_fault_detail;
#[path = "c21_domain.rs"]
mod c21_domain;
#[path = "c21_host_policy.rs"]
mod c21_host_policy;
#[path = "c23_introspect.rs"]
mod c23_introspect;
#[path = "d0_positions.rs"]
mod d0_positions;
#[path = "j1_recovery.rs"]
mod j1_recovery;
#[path = "j4_group_metadata.rs"]
mod j4_group_metadata;
#[path = "made_plan.rs"]
mod made_plan;
