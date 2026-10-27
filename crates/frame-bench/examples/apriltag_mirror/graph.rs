//! The graph of the mirror: the tracked group's stages in an embedded graph (as the planner expands
//! it on the live engine), the pose nodes, and the host bridge edges.

use daedalus::{
    PortHandle,
    data::model::{TypeExpr, Value},
    planner::Graph,
    registry::capability::{NodeDecl, PortDecl},
    runtime::{
        EMBEDDED_GRAPH_KEY, EMBEDDED_HOST_KEY,
        graph_builder::{GraphCtx, graph_to_json},
        plugins::PluginRegistry,
    },
    transport::FRAME_INTERFACE_KEY,
};
use daedalus_frame_bench::frame_bench_registry;

use crate::stages::{Blob, MirrorPlugin};
use crate::{BLOB_KEY, CAMERA, EXTRINSICS, Error, FRAME, PLUGIN};

/// The tracked group's 19 stages, `(node id, alias)` in pipeline order (`eidos_group.rs`).
const GROUP_STAGES: [(&str, &str); 19] = [
    ("detectors.track_plan", "plan"),
    ("aruco.mask_prep_runs", "full_mask_prep"),
    ("aruco.quads_from_runs", "full_quads"),
    ("detectors.track_windows", "windows"),
    ("aruco.decode", "decode"),
    ("aruco.validate", "validate"),
    ("aruco.refine", "refine"),
    ("detectors.track_fine_windows", "fine_windows"),
    ("detectors.track_loss", "loss"),
    ("aruco.mask_prep_runs", "same_frame_mask_prep"),
    ("aruco.quads_from_runs", "same_frame_quads"),
    ("detectors.track_windows", "second_windows"),
    ("aruco.decode", "second_decode"),
    ("aruco.validate", "second_validate"),
    ("aruco.refine", "second_refine"),
    ("detectors.track_fine_windows", "second_fine_windows"),
    ("detectors.track_coarse", "coarse"),
    ("detectors.track_fine_band", "fine_band"),
    ("detectors.track_update", "update"),
];

/// The group node (`eidos:detectors.apriltag_tracked`): its embedded graph is the stages above,
/// and the planner expands it, so every stage carries the group's metadata as on the live engine.
const GROUP_ID: &str = "eidos:detectors.apriltag_tracked";
/// The top-level pose and multi-tag pose nodes (outside the group, as in `edges.txt`).
const POSE_ID: &str = "eidos:aruco.pose";
const MULTI_ID: &str = "eidos:aruco.multi_tag_pose";
/// Output ports of the group, bound to the update stage.
const GROUP_OUTPUTS: [&str; 3] = ["detections", "rejected", "refined_corners"];

fn port(alias: &str, name: &str) -> PortHandle {
    PortHandle::new(alias, name)
}

fn err(error: impl ToString) -> Error {
    error.to_string().into()
}

pub(crate) fn mirror_registry() -> Result<PluginRegistry, Error> {
    let mut registry = frame_bench_registry()?;
    registry.install(&MirrorPlugin::new())?;
    register_group(&mut registry)?;
    Ok(registry)
}

/// Registers [`GROUP_ID`]: the 19 stages in an embedded graph (`eidos_group.rs`'s tracked group),
/// with the plan's const inputs, decode's fixed `validate` off and the quads' `subpixel` on.
fn register_group(registry: &mut PluginRegistry) -> Result<(), Error> {
    let capabilities = registry.combined_transport_capabilities().map_err(err)?;
    let mut ctx = GraphCtx::new(capabilities, &[FRAME], &GROUP_OUTPUTS);
    let mut handles = Vec::with_capacity(GROUP_STAGES.len());
    for (id, alias) in GROUP_STAGES {
        handles.push(
            ctx.try_node_as(&format!("{PLUGIN}:{id}"), alias)
                .map_err(err)?,
        );
    }
    let index = |alias: &str| {
        GROUP_STAGES
            .iter()
            .position(|(_, stage)| *stage == alias)
            .ok_or_else(|| err(format!("group stage {alias}")))
    };
    let node = |alias: &str| -> Result<&_, Error> { Ok(&handles[index(alias)?]) };
    let frame = ctx.input(FRAME);
    let mut edges = Vec::new();
    {
        let mut edge =
            |from: &str, from_port: &str, to: &str, to_port: &str| -> Result<(), Error> {
                let source = if from == "host" {
                    frame.clone()
                } else {
                    node(from)?.output(from_port)
                };
                edges.push((source, node(to)?.input(to_port)));
                Ok(())
            };
        edge("host", "frame", "plan", "frame")?;
        // Full search.
        edge("plan", "full_frame", "full_mask_prep", "frame")?;
        edge("full_mask_prep", "runs", "full_quads", "runs")?;
        // Window search, or the full search's quads passed on.
        edge("host", "frame", "windows", "frame")?;
        edge("plan", "windows", "windows", "windows")?;
        edge("full_quads", "quads", "windows", "full_quads")?;
        for (prefix, quads) in [("", "windows"), ("second_", "second_windows")] {
            let decode = format!("{prefix}decode");
            let validate = format!("{prefix}validate");
            let refine = format!("{prefix}refine");
            edge("host", "frame", &decode, "frame")?;
            edge("host", "frame", &validate, "frame")?;
            edge("host", "frame", &refine, "frame")?;
            edge(quads, "quads", &decode, "quads")?;
            edge(&decode, "detections", &validate, "detections")?;
            edge(&validate, "detections", &refine, "detections")?;
        }
        // The full-resolution fallback of each pass.
        for (prefix, windows) in [
            ("", ("plan", "windows")),
            ("second_", ("loss", "recovery_windows")),
        ] {
            let fine = format!("{prefix}fine_windows");
            edge("host", "frame", &fine, "frame")?;
            edge("plan", "track", &fine, "track")?;
            edge(windows.0, windows.1, &fine, "windows")?;
            edge(
                &format!("{prefix}validate"),
                "detections",
                &fine,
                "detections",
            )?;
            edge(&format!("{prefix}validate"), "rejected", &fine, "rejected")?;
            edge(
                &format!("{prefix}refine"),
                "refined_corners",
                &fine,
                "refined_corners",
            )?;
        }
        edge("plan", "full_frame", "fine_windows", "full_frame")?;
        // Loss.
        edge("host", "frame", "loss", "frame")?;
        edge("plan", "track", "loss", "track")?;
        edge("plan", "windows", "loss", "windows")?;
        edge("fine_windows", "detections", "loss", "detections")?;
        edge("fine_windows", "refined_corners", "loss", "refined_corners")?;
        edge("loss", "full_frame", "same_frame_mask_prep", "frame")?;
        edge("same_frame_mask_prep", "runs", "same_frame_quads", "runs")?;
        edge("loss", "second_frame", "second_windows", "frame")?;
        edge("loss", "recovery_windows", "second_windows", "windows")?;
        edge("same_frame_quads", "quads", "second_windows", "full_quads")?;
        edge("plan", "track", "second_fine_windows", "track")?;
        edge("loss", "recovery_windows", "second_fine_windows", "windows")?;
        edge(
            "second_validate",
            "detections",
            "second_fine_windows",
            "detections",
        )?;
        edge(
            "second_validate",
            "rejected",
            "second_fine_windows",
            "rejected",
        )?;
        edge(
            "second_refine",
            "refined_corners",
            "second_fine_windows",
            "refined_corners",
        )?;
        // Coarse pass and full-resolution band.
        edge("host", "frame", "coarse", "frame")?;
        edge("plan", "track", "coarse", "track")?;
        edge("plan", "windows", "coarse", "windows")?;
        edge("loss", "second_frame", "coarse", "second_frame")?;
        edge("host", "frame", "fine_band", "frame")?;
        edge("plan", "track", "fine_band", "track")?;
        edge("plan", "windows", "fine_band", "windows")?;
        edge("loss", "second_frame", "fine_band", "second_frame")?;
        // Update.
        edge("plan", "track", "update", "track")?;
        edge("fine_windows", "detections", "update", "detections")?;
        edge("fine_windows", "rejected", "update", "rejected")?;
        edge(
            "fine_windows",
            "refined_corners",
            "update",
            "refined_corners",
        )?;
        edge("second_windows", "quads", "update", "second_quads")?;
        edge(
            "second_fine_windows",
            "detections",
            "update",
            "second_detections",
        )?;
        edge(
            "second_fine_windows",
            "rejected",
            "update",
            "second_rejected",
        )?;
        edge(
            "second_fine_windows",
            "refined_corners",
            "update",
            "second_refined_corners",
        )?;
        edge("coarse", "detections", "update", "coarse_detections")?;
        edge("coarse", "rejected", "update", "coarse_rejected")?;
        edge(
            "coarse",
            "refined_corners",
            "update",
            "coarse_refined_corners",
        )?;
        edge("fine_band", "detections", "update", "band_detections")?;
        edge("fine_band", "rejected", "update", "band_rejected")?;
        edge(
            "fine_band",
            "refined_corners",
            "update",
            "band_refined_corners",
        )?;
    }
    for (from, to) in &edges {
        ctx.try_connect(from, to).map_err(err)?;
    }
    let p = |alias: &str, name: &str| node(alias).map(|handle| handle.input(name));
    ctx.try_const_input(&p("plan", "full_search_every")?, Value::Int(5))
        .map_err(err)?;
    ctx.try_const_input(&p("plan", "margin_growth_misses")?, Value::Int(6))
        .map_err(err)?;
    ctx.try_const_input(&p("plan", "loss_full_search_after")?, Value::Int(0))
        .map_err(err)?;
    ctx.try_const_input(&p("plan", "track_margin")?, Value::Float(1.5))
        .map_err(err)?;
    ctx.try_const_input(&p("plan", "tracked")?, Value::Bool(true))
        .map_err(err)?;
    for alias in ["decode", "second_decode"] {
        ctx.try_const_input(&p(alias, "validate")?, Value::Bool(false))
            .map_err(err)?;
    }
    for alias in [
        "full_quads",
        "same_frame_quads",
        "windows",
        "second_windows",
    ] {
        ctx.try_const_input(&p(alias, "subpixel")?, Value::Bool(true))
            .map_err(err)?;
    }
    for (name, alias, port) in [
        ("detections", "update", "detections"),
        ("rejected", "update", "rejected"),
        ("refined_corners", "update", "refined_corners"),
    ] {
        ctx.try_bind_output(name, &node(alias)?.output(port))
            .map_err(err)?;
    }
    let graph = ctx.try_build().map_err(err)?;
    let graph_json = graph_to_json(&graph).map_err(err)?;
    let mut decl = NodeDecl::new(GROUP_ID)
        .label("AprilTag detector, tracked")
        .input(
            PortDecl::new(FRAME, FRAME_INTERFACE_KEY)
                .schema(TypeExpr::Opaque(FRAME_INTERFACE_KEY.into())),
        );
    for name in GROUP_OUTPUTS {
        decl = decl.output(PortDecl::new(name, BLOB_KEY).schema(TypeExpr::Opaque(BLOB_KEY.into())));
    }
    decl = decl
        .metadata(EMBEDDED_GRAPH_KEY, Value::String(graph_json.into()))
        .metadata(EMBEDDED_HOST_KEY, Value::String("host".into()));
    registry.register_node_decl(decl).map_err(err)
}

/// The top-level graph: the host's frame into the group, the group's outputs to the host and to
/// the pose nodes, and the camera and extrinsics into the pose nodes.
pub(crate) fn mirror_graph(registry: &PluginRegistry) -> Result<Graph, Error> {
    let p = port;
    let builder = registry
        .graph_builder()?
        .input_as(FRAME, TypeExpr::opaque(FRAME_INTERFACE_KEY))
        .input_typed::<Blob>(CAMERA)?
        .input_typed::<Blob>(EXTRINSICS)?
        .try_node_id(GROUP_ID, "detector")?
        .try_node_id(POSE_ID, "pose")?
        .try_node_id(MULTI_ID, "multi_tag_pose")?
        .try_connect(FRAME, &p("detector", "frame"))?
        // Host bridge outputs (edges.txt 13..15).
        .try_connect(&p("detector", "detections"), "detections")?
        .try_connect(&p("detector", "rejected"), "rejected")?
        .try_connect(&p("detector", "refined_corners"), "refined_corners")?
        // Pose and multi-tag pose (edges.txt 16..25).
        .try_connect(&p("detector", "detections"), &p("pose", "detections"))?
        .try_connect(
            &p("detector", "refined_corners"),
            &p("pose", "refined_corners"),
        )?
        .try_connect(CAMERA, &p("pose", "camera"))?
        .try_connect(&p("pose", "poses"), "poses")?
        .try_connect(&p("pose", "solutions"), "pose_solutions")?
        .try_connect(
            &p("detector", "detections"),
            &p("multi_tag_pose", "detections"),
        )?
        .try_connect(
            &p("detector", "refined_corners"),
            &p("multi_tag_pose", "refined_corners"),
        )?
        .try_connect(CAMERA, &p("multi_tag_pose", "camera"))?
        .try_connect(EXTRINSICS, &p("multi_tag_pose", "extrinsics"))?
        .try_connect(&p("multi_tag_pose", "camera_pose"), "multi_tag_pose")?;
    Ok(builder.build())
}
