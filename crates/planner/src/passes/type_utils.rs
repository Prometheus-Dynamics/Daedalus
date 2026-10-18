use alloc::borrow::Cow;
use alloc::string::ToString;
use daedalus_data::model::TypeExpr;
use daedalus_registry::capability::{NodeDecl, PortDecl};

use crate::graph::{ComputeAffinity, NodeInstance};
use crate::metadata::{dynamic_port_type, is_generic_marker};

use super::{PlannerCatalog, PlannerConfig};

/// The type of a node port. Borrowed from the declaration when the declaration has the type,
/// owned when it is resolved from node metadata or derived from a type key.
pub(super) fn port_type<'a>(
    node: &NodeInstance,
    desc: &'a NodeDecl,
    name: &str,
    is_input: bool,
) -> Option<Cow<'a, TypeExpr>> {
    if is_input {
        if let Some(ty) = dynamic_port_type(&node.metadata, true, name) {
            return Some(Cow::Owned(ty));
        }
        if let Some(ty) = input_ty_for(desc, name) {
            if is_generic_marker(&ty)
                && let Some(solved) = dynamic_port_type(&node.metadata, true, name)
            {
                return Some(Cow::Owned(solved));
            }
            return Some(ty);
        }
    } else if let Some(ty) = dynamic_port_type(&node.metadata, false, name) {
        return Some(Cow::Owned(ty));
    } else if let Some(port) = desc.outputs.iter().find(|p| p.name == name) {
        let ty = port_schema(port);
        if is_generic_marker(&ty)
            && let Some(solved) = dynamic_port_type(&node.metadata, false, name)
        {
            return Some(Cow::Owned(solved));
        }
        return Some(ty);
    }
    // Dynamic port declarations are trusted only from the registry descriptor, not per-node
    // graph metadata (which may come from untrusted UI/clients).
    crate::metadata::descriptor_dynamic_port_type(desc, is_input)
        .map(|key| Cow::Owned(TypeExpr::Opaque(key)))
}

pub(super) fn port_schema(port: &PortDecl) -> Cow<'_, TypeExpr> {
    match &port.schema {
        Some(schema) => Cow::Borrowed(schema),
        None => Cow::Owned(TypeExpr::Opaque(port.type_key.to_string())),
    }
}

pub(super) fn input_ty_for<'a>(desc: &'a NodeDecl, name: &str) -> Option<Cow<'a, TypeExpr>> {
    if let Some(port) = desc
        .inputs
        .iter()
        .find(|port| port.name.eq_ignore_ascii_case(name))
    {
        return Some(port_schema(port));
    }
    desc.fanin_inputs.iter().find_map(|spec| {
        name.strip_prefix(&spec.prefix)
            .and_then(|rest| rest.parse::<u32>().ok())
            .filter(|index| *index >= spec.start)
            .map(|_| match &spec.schema {
                Some(schema) => Cow::Borrowed(schema),
                None => Cow::Owned(TypeExpr::Opaque(spec.type_key.to_string())),
            })
    })
}

pub(super) fn input_access_for(desc: &NodeDecl, name: &str) -> daedalus_transport::AccessMode {
    desc.inputs
        .iter()
        .find(|port| port.name.eq_ignore_ascii_case(name))
        .map(|port| port.access)
        .unwrap_or(daedalus_transport::AccessMode::Read)
}

pub(super) fn adapt_request_for_input(
    catalog: &PlannerCatalog,
    access: daedalus_transport::AccessMode,
    ty: &TypeExpr,
) -> daedalus_transport::AdaptRequest {
    let mut request = daedalus_transport::AdaptRequest::new(catalog.transport_key(ty));
    request.access = access;
    request
}

pub(super) fn target_residency_for_node(
    node: &NodeInstance,
    config: &PlannerConfig,
) -> Option<daedalus_transport::Residency> {
    match node.compute {
        ComputeAffinity::GpuRequired | ComputeAffinity::GpuPreferred if config.enable_gpu => {
            Some(daedalus_transport::Residency::Gpu)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generic_marker_is_case_insensitive_opaque_generic() {
        assert!(is_generic_marker(&TypeExpr::Opaque("generic".to_string())));
        assert!(is_generic_marker(&TypeExpr::Opaque("Generic".to_string())));
        assert!(!is_generic_marker(&TypeExpr::Opaque("frame".to_string())));
        assert!(!is_generic_marker(&TypeExpr::Scalar(
            daedalus_data::model::ValueType::String
        )));
    }
}
