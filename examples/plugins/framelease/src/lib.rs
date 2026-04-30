#[cfg(feature = "styx-framelease")]
mod framelease_plugin {
    use daedalus::{declare_plugin, macros::node, runtime::NodeError};
    use styx::prelude::*;

    #[node(id = "mark_timestamp", inputs("frame"), outputs("frame"))]
    fn mark_timestamp(mut frame: FrameLease) -> Result<FrameLease, NodeError> {
        frame.meta_mut().timestamp = frame.meta().timestamp.saturating_add(1);
        Ok(frame)
    }

    #[node(id = "touch_first_byte", inputs("frame"), outputs("frame"))]
    fn touch_first_byte(mut frame: FrameLease) -> Result<FrameLease, NodeError> {
        if let Some(mut plane) = frame.planes_mut().into_iter().next()
            && let Some(first) = plane.data().first_mut()
        {
            *first = first.saturating_add(1);
        }
        Ok(frame)
    }

    declare_plugin!(
        FrameLeaseDynamicPlugin,
        "framelease_dynamic",
        [mark_timestamp, touch_first_byte]
    );

    daedalus::export_plugin!(FrameLeaseDynamicPlugin);
}
