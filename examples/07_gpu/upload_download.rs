use daedalus::{
    data::daedalus_type::DaedalusTypeExpr,
    runtime::plugins::PluginRegistry,
    transport::{DeviceClass, TransferFrom, TransferTo, TransportError},
    type_key,
};

#[type_key("example:frame")]
#[derive(Clone)]
struct Frame(Vec<u8>);

#[type_key("example:frame@mock")]
#[derive(Clone)]
struct MockFrame(Vec<u8>);

struct MockDevice;

impl DeviceClass for MockDevice {
    const ID: &'static str = "mock";
    type Context = ();
}

impl TransferTo<MockDevice> for Frame {
    type Resident = MockFrame;

    fn transfer_to(&self, _ctx: &()) -> Result<Self::Resident, TransportError> {
        Ok(MockFrame(self.0.clone()))
    }
}

impl TransferFrom<MockDevice> for Frame {
    type Resident = MockFrame;

    fn transfer_from(resident: &Self::Resident, _ctx: &()) -> Result<Self, TransportError> {
        Ok(Frame(resident.0.clone()))
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    let spec = registry.register_device_transfer::<MockDevice, Frame>(
        Frame::type_expr(),
        MockFrame::type_expr(),
        (),
    )?;
    println!("upload adapter: {}", spec.upload_id);
    println!("download adapter: {}", spec.download_id);
    Ok(())
}
