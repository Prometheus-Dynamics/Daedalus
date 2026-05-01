use daedalus_data::model::TypeExpr;
use daedalus_registry::capability::{NodeDecl, PortDecl};

use crate::graph::{ComputeAffinity, NodeInstance};
use crate::metadata::DynamicPortMetadata;

use super::PlannerConfig;

pub(super) fn is_generic_marker(ty: &TypeExpr) -> bool {
    matches!(ty, TypeExpr::Opaque(value) if value.eq_ignore_ascii_case("generic"))
}

pub(super) fn port_type(
    node: &NodeInstance,
    desc: &NodeDecl,
    name: &str,
    is_input: bool,
) -> Option<TypeExpr> {
    let dynamic_metadata = DynamicPortMetadata::from_node_metadata(&node.metadata);

    if is_input {
        if let Some(ty) = dynamic_metadata.resolved_type(true, name) {
            return Some(ty);
        }
        if let Some(ty) = input_ty_for(desc, name) {
            if is_generic_marker(&ty)
                && let Some(solved) = dynamic_metadata.resolved_type(true, name)
            {
                return Some(solved);
            }
            return Some(ty);
        }
    } else if let Some(ty) = dynamic_metadata.resolved_type(false, name) {
        return Some(ty);
    } else if let Some(port) = desc.outputs.iter().find(|p| p.name == name) {
        let ty = port_schema(port);
        if is_generic_marker(&ty)
            && let Some(solved) = dynamic_metadata.resolved_type(false, name)
        {
            return Some(solved);
        }
        return Some(ty);
    }
    // Dynamic port declarations are trusted only from the registry descriptor, not per-node
    // graph metadata (which may come from untrusted UI/clients).
    crate::metadata::descriptor_dynamic_port_type(desc, is_input).map(TypeExpr::Opaque)
}

pub(super) fn port_schema(port: &PortDecl) -> TypeExpr {
    port.schema
        .clone()
        .unwrap_or_else(|| TypeExpr::Opaque(port.type_key.to_string()))
}

pub(super) fn input_ty_for(desc: &NodeDecl, name: &str) -> Option<TypeExpr> {
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
            .map(|_| {
                spec.schema
                    .clone()
                    .unwrap_or_else(|| TypeExpr::Opaque(spec.type_key.to_string()))
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
    access: daedalus_transport::AccessMode,
    ty: &TypeExpr,
) -> daedalus_transport::AdaptRequest {
    let mut request = daedalus_transport::AdaptRequest::new(typeexpr_transport_key(ty));
    request.access = access;
    request
}

pub(super) fn typeexpr_transport_key(ty: &TypeExpr) -> daedalus_transport::TypeKey {
    daedalus_registry::typeexpr_transport_key(ty)
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
