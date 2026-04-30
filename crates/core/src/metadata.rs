/// Metadata key attached to host-bridge descriptors to mark runtime host I/O nodes.
pub const HOST_BRIDGE_META_KEY: &str = "host_bridge";

/// Registry descriptor metadata key that allows arbitrary dynamic input ports.
pub const DYNAMIC_INPUTS_KEY: &str = "dynamic_inputs";
/// Registry descriptor metadata key that allows arbitrary dynamic output ports.
pub const DYNAMIC_OUTPUTS_KEY: &str = "dynamic_outputs";

/// Planner-owned node metadata keys for resolved dynamic input/output schemas and labels.
pub const DYNAMIC_INPUT_TYPES_KEY: &str = "dynamic_input_types";
pub const DYNAMIC_OUTPUT_TYPES_KEY: &str = "dynamic_output_types";
pub const DYNAMIC_INPUT_LABELS_KEY: &str = "dynamic_input_labels";
pub const DYNAMIC_OUTPUT_LABELS_KEY: &str = "dynamic_output_labels";

/// Planner/runtime-owned metadata keys attached when embedded graphs are expanded.
pub const EMBEDDED_GROUP_KEY: &str = "daedalus.embedded_group";
pub const GROUP_ID_KEY: &str = "daedalus.group_id";
pub const GROUP_LABEL_KEY: &str = "daedalus.group_label";

/// UI-provided node id used only as a human-readable diagnostic fallback.
pub const UI_NODE_ID_KEY: &str = "helios.ui.node_id";

/// Planner-owned graph metadata key containing applied lowering records.
pub const PLAN_APPLIED_LOWERINGS_KEY: &str = "daedalus.plan.applied_lowerings";
/// Planner-owned graph metadata key containing edge transport/conversion explanations.
pub const PLAN_EDGE_EXPLANATIONS_KEY: &str = "daedalus.plan.edge_explanations";
/// Planner-owned graph metadata key containing selected overload information.
pub const PLAN_OVERLOAD_RESOLUTIONS_KEY: &str = "daedalus.plan.overload_resolutions";
/// Prefix for transient planner converter metadata.
pub const PLAN_CONVERTER_METADATA_PREFIX: &str = "converter:";

/// Planner/runtime graph metadata key for topological node ordering.
pub const PLAN_TOPO_ORDER_KEY: &str = "topo_order";
/// Planner/runtime graph metadata key for final schedule ordering.
pub const PLAN_SCHEDULE_ORDER_KEY: &str = "schedule_order";
/// Planner-owned graph metadata key for schedule priority diagnostics.
pub const PLAN_SCHEDULE_PRIORITY_KEY: &str = "schedule_priority";
/// Planner-owned graph metadata key for GPU segment diagnostics.
pub const PLAN_GPU_SEGMENTS_KEY: &str = "gpu_segments";
/// Planner-owned graph metadata key for GPU rejection/selection rationale.
pub const PLAN_GPU_WHY_KEY: &str = "gpu_why";
