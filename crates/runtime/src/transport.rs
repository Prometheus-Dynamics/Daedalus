use daedalus_data::model::TypeExpr;
use daedalus_transport::{
    AccessMode, AdaptRequest, AdapterId, AdapterTable, Payload, TransportError, TypeKey,
};

pub fn typeexpr_transport_key(ty: &TypeExpr) -> Result<TypeKey, TransportError> {
    Ok(daedalus_registry::typeexpr_transport_key(ty))
}

/// Runtime-owned executable transport adapter table.
#[derive(Clone, Debug, Default)]
pub struct RuntimeTransport {
    adapters: AdapterTable,
}

impl RuntimeTransport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn adapters(&self) -> &AdapterTable {
        &self.adapters
    }

    pub fn adapters_mut(&mut self) -> &mut AdapterTable {
        &mut self.adapters
    }

    pub fn register_adapter_fn<F>(
        &mut self,
        id: impl Into<AdapterId>,
        f: F,
    ) -> Result<(), TransportError>
    where
        F: Fn(Payload, &AdaptRequest) -> Result<Payload, TransportError> + Send + Sync + 'static,
    {
        self.adapters.register_fn(id, f)
    }

    pub fn execute_adapter_path(
        &self,
        mut payload: Payload,
        steps: &[AdapterId],
        request: &AdaptRequest,
    ) -> Result<Payload, TransportError> {
        if cached_resident_can_satisfy(&payload, request, steps.is_empty())
            && let Some(resident) = cached_resident_for_request(&payload, request)
        {
            return Ok(resident);
        }
        for step in steps {
            payload = self.adapters.adapt(step, payload, request)?;
            if cached_resident_can_satisfy(&payload, request, false)
                && let Some(resident) = cached_resident_for_request(&payload, request)
            {
                return Ok(resident);
            }
        }
        Ok(payload)
    }
}

fn cached_resident_can_satisfy(
    payload: &Payload,
    request: &AdaptRequest,
    empty_path: bool,
) -> bool {
    empty_path
        || (matches!(request.access, AccessMode::Read | AccessMode::View)
            && (request.residency.is_some() || payload.type_key() != &request.target))
}

fn cached_resident_for_request(payload: &Payload, request: &AdaptRequest) -> Option<Payload> {
    if let Some(residency) = request.residency {
        return payload.resident(&request.target, residency, request.layout.as_ref());
    }
    payload.resident_by_type(&request.target, request.layout.as_ref())
}
