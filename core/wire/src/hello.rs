//! The hello: the first packet a worker sends. It names the protocol, the layout of the messages,
//! and every node it serves, so the host can check them against its registry.

use daedalus_types::TypeExpr;

use crate::codec::{Reader, Writer};
use crate::schema::Schema;
use crate::value::{WireType, WireValue, decode_value, encode_value};
use crate::{LAYOUT_HASH, PROTOCOL_VERSION, WireError};

/// One port: a name, its type, and for a config port its default.
#[derive(Clone, Debug, PartialEq)]
pub struct PortSpec {
    pub name: String,
    pub ty: WireType,
    pub default: Option<WireValue>,
}

impl PortSpec {
    pub fn new(name: &str, ty: WireType) -> PortSpec {
        PortSpec {
            name: name.to_owned(),
            ty,
            default: None,
        }
    }

    /// A port whose type comes from an A4 type expression.
    pub fn of(name: &str, expr: &TypeExpr) -> PortSpec {
        PortSpec::new(name, WireType::from_expr(expr))
    }
}

/// One node a worker serves.
#[derive(Clone, Debug, PartialEq)]
pub struct Declaration {
    pub id: String,
    pub inputs: Vec<PortSpec>,
    pub outputs: Vec<PortSpec>,
    pub config: Vec<PortSpec>,
    /// The state type, when the node keeps state between runs.
    pub state: Option<WireType>,
    pub capabilities: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Hello {
    pub protocol: u32,
    pub layout: u64,
    pub worker: String,
    /// The largest packet this worker will accept.
    pub packet_cap: u32,
    pub nodes: Vec<Declaration>,
}

impl Hello {
    /// A hello for this build of the protocol.
    pub fn new(worker: &str, packet_cap: u32, nodes: Vec<Declaration>) -> Hello {
        Hello {
            protocol: PROTOCOL_VERSION,
            layout: LAYOUT_HASH,
            worker: worker.to_owned(),
            packet_cap,
            nodes,
        }
    }

    /// Refuses a hello with another protocol version or layout hash. The error names both values.
    pub fn check(&self) -> Result<(), WireError> {
        if self.protocol != PROTOCOL_VERSION {
            return Err(WireError::Protocol {
                expected: PROTOCOL_VERSION,
                found: self.protocol,
            });
        }
        if self.layout != LAYOUT_HASH {
            return Err(WireError::Layout {
                expected: LAYOUT_HASH,
                found: self.layout,
            });
        }
        Ok(())
    }

    pub fn encode(&self, out: &mut Writer) -> Result<(), WireError> {
        out.u32(self.protocol);
        out.u64(self.layout);
        out.str(&self.worker)?;
        out.u32(self.packet_cap);
        out.len(self.nodes.len())?;
        for node in &self.nodes {
            out.str(&node.id)?;
            for ports in [&node.inputs, &node.outputs, &node.config] {
                out.len(ports.len())?;
                for port in ports {
                    encode_port(port, out)?;
                }
            }
            match &node.state {
                None => out.u8(0),
                Some(state) => {
                    out.u8(1);
                    state.encode(out)?;
                }
            }
            out.len(node.capabilities.len())?;
            for capability in &node.capabilities {
                out.str(capability)?;
            }
        }
        Ok(())
    }

    pub fn decode(input: &mut Reader<'_>) -> Result<Hello, WireError> {
        let protocol = input.u32()?;
        let layout = input.u64()?;
        let worker = input.str()?;
        let packet_cap = input.u32()?;
        let count = input.count()?;
        let mut nodes = Vec::with_capacity(count);
        for _ in 0..count {
            let id = input.str()?;
            let inputs = decode_ports(input)?;
            let outputs = decode_ports(input)?;
            let config = decode_ports(input)?;
            let state = match input.u8()? {
                0 => None,
                1 => Some(WireType::decode(input, 0)?),
                tag => {
                    return Err(WireError::BadTag {
                        what: "state flag",
                        tag: u32::from(tag),
                    });
                }
            };
            let capability_count = input.count()?;
            let mut capabilities = Vec::with_capacity(capability_count);
            for _ in 0..capability_count {
                capabilities.push(input.str()?);
            }
            nodes.push(Declaration {
                id,
                inputs,
                outputs,
                config,
                state,
                capabilities,
            });
        }
        Ok(Hello {
            protocol,
            layout,
            worker,
            packet_cap,
            nodes,
        })
    }
}

fn encode_port(port: &PortSpec, out: &mut Writer) -> Result<(), WireError> {
    out.str(&port.name)?;
    port.ty.encode(out)?;
    match &port.default {
        None => out.u8(0),
        Some(value) => {
            // A default is written as the port's own type, with an empty schema: a default that
            // needs a codec is refused here, not at the far side.
            out.u8(1);
            encode_value(&port.ty, value, &Schema::new(), out)?;
        }
    }
    Ok(())
}

fn decode_ports(input: &mut Reader<'_>) -> Result<Vec<PortSpec>, WireError> {
    let count = input.count()?;
    let mut ports = Vec::with_capacity(count);
    for _ in 0..count {
        let name = input.str()?;
        let ty = WireType::decode(input, 0)?;
        let default = match input.u8()? {
            0 => None,
            1 => Some(decode_value(&ty, input, &Schema::new())?),
            tag => {
                return Err(WireError::BadTag {
                    what: "default flag",
                    tag: u32::from(tag),
                });
            }
        };
        ports.push(PortSpec { name, ty, default });
    }
    Ok(ports)
}
