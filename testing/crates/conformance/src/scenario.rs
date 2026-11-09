//! The nodes a check offers and compiles.

use std::any::Any;

use daedalus_backend::{
    Backend, BodyIndex, CompileCx, ConstOf, NodeBody, NodeRef, Offer, OfferNode, PortInfo, PortRef,
    RateId, Region,
};
use daedalus_types::{BackendIndex, NodeIndex, PortIndex, Scalar, TypeExpr, TypeIndex};
use daedalus_value::Access;

const NODES: u32 = 3;

static PORT_IN: [PortInfo<'static>; 1] = [PortInfo {
    name: "in",
    ty: TypeIndex(0),
    access: Access::Read,
    optional: false,
}];
static PORT_OUT: [PortInfo<'static>; 1] = [PortInfo {
    name: "out",
    ty: TypeIndex(0),
    access: Access::Read,
    optional: false,
}];
static TYPES: [TypeExpr; 1] = [TypeExpr::Scalar(Scalar::U8)];
static CONSTS: [ConstOf<'static>; 0] = [];

/// What a backend's bodies look like. The default suits a backend that ignores them.
#[derive(Clone, Copy, Debug)]
pub struct Scenario {
    pub code: &'static (dyn Any + Send + Sync),
    pub text: &'static str,
    pub needs: &'static [&'static str],
}

impl Default for Scenario {
    fn default() -> Scenario {
        Scenario {
            code: &(),
            text: "",
            needs: &[],
        }
    }
}

impl Scenario {
    pub(crate) fn offered(&self) -> Vec<OfferNode<'static>> {
        (0..NODES)
            .map(|node| OfferNode {
                node: NodeIndex(node),
                body: BodyIndex(0),
                inputs: &PORT_IN,
                outputs: &PORT_OUT,
                needs: self.needs,
            })
            .collect()
    }

    /// What the backend takes of the standard offer.
    pub(crate) fn taken(&self, backend: &dyn Backend) -> Vec<NodeRef> {
        backend.can_take(&Offer {
            nodes: &self.offered(),
        })
    }

    /// Calls `body` with a region over `nodes` and everything compiling it takes.
    pub(crate) fn with_region<R>(
        &self,
        backend: &dyn Backend,
        nodes: &[NodeIndex],
        body: impl FnOnce(&Region<'_>, &CompileCx<'_>) -> R,
    ) -> R {
        let port = |node: NodeIndex| PortRef {
            node,
            port: PortIndex(0),
        };
        let ports: Vec<PortRef> = nodes.iter().map(|node| port(*node)).collect();
        let region = Region {
            backend: BackendIndex(0),
            nodes,
            inputs: &ports,
            outputs: &ports,
            incoming: &[],
            rate: RateId(0),
        };
        let spaces = backend.memory();
        let bodies: Vec<NodeBody<'_>> = nodes
            .iter()
            .map(|node| NodeBody {
                node: *node,
                body: BodyIndex(0),
                code: self.code,
                text: self.text,
            })
            .collect();
        let cx = CompileCx {
            types: &TYPES,
            consts: &CONSTS,
            spaces: &spaces,
            bodies: &bodies,
            merge_allowed: true,
        };
        body(&region, &cx)
    }

    pub(crate) fn all_nodes() -> Vec<NodeIndex> {
        (0..NODES).map(NodeIndex).collect()
    }
}
