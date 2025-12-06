use daedalus::data::model::TypeExpr;
use daedalus::registry::store::{NodeDescriptor, Port, Registry};
use daedalus::runtime::handler_registry::HandlerRegistry;
use daedalus::runtime::{EdgePayload, NodeError};
use daedalus::runtime::{ExecutionContext, NodeIo, RuntimeNode};
use daedalus::ComputeAffinity;

#[derive(Clone, Debug)]
pub struct Frame {
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct Detection {
    pub id: i32,
}

pub fn register_bundle(reg: &mut Registry) -> Result<(), &'static str> {
    reg.register_node(NodeDescriptor {
        id: "frame_src".into(),
        feature_flags: vec![],
        label: None,
        inputs: vec![],
        fanin_inputs: vec![],
        outputs: vec![Port {
            name: "frame".into(),
            ty: TypeExpr::Bytes,
            source: None,
            const_value: None,
        }],
        default_compute: ComputeAffinity::CpuOnly,
        sync_groups: Vec::new(),
        metadata: Default::default(),
    })
    .map_err(|_| "registry conflict")?;
    reg.register_node(NodeDescriptor {
        id: "decode".into(),
        feature_flags: vec![],
        label: None,
        inputs: vec![Port {
            name: "frame".into(),
            ty: TypeExpr::Bytes,
            source: None,
            const_value: None,
        }],
        fanin_inputs: vec![],
        outputs: vec![Port {
            name: "detections".into(),
            ty: TypeExpr::Bytes,
            source: None,
            const_value: None,
        }],
        default_compute: ComputeAffinity::CpuOnly,
        sync_groups: Vec::new(),
        metadata: Default::default(),
    })
    .map_err(|_| "registry conflict")?;
    reg.register_node(NodeDescriptor {
        id: "sink".into(),
        feature_flags: vec![],
        label: None,
        inputs: vec![Port {
            name: "detections".into(),
            ty: TypeExpr::Bytes,
            source: None,
            const_value: None,
        }],
        fanin_inputs: vec![],
        outputs: vec![],
        default_compute: ComputeAffinity::CpuOnly,
        sync_groups: Vec::new(),
        metadata: Default::default(),
    })
    .map_err(|_| "registry conflict")?;
    Ok(())
}

pub fn bundle_handlers() -> HandlerRegistry {
    let mut reg = HandlerRegistry::new();
    reg.on("frame_src", |_n, _c, io| {
        io.push_any(
            Some("frame"),
            Frame {
                bytes: b"raw_frame".to_vec(),
            },
        );
        Ok(())
    });
    reg.on("decode", |_n, _c, io| {
        let frame = io
            .get_any::<Frame>("frame")
            .cloned()
            .ok_or_else(|| NodeError::InvalidInput("missing frame".into()))?;
        let dets = vec![
            Detection {
                id: frame.bytes.len() as i32,
            },
            Detection { id: 7 },
        ];
        io.push_any(Some("detections"), dets);
        Ok(())
    });
    reg.on("sink", |_n, _c, io| {
        if let Some(dets) = io.get_any::<Vec<Detection>>("detections") {
            println!("detections: {:?}", dets);
        }
        Ok(())
    });
    reg
}
