use super::*;

#[derive(Clone, Debug)]
pub struct PayloadLease {
    pub id: String,
    pub payload: Payload,
    pub scope: PayloadLeaseScope,
    pub created_at: Instant,
    pub last_used: Instant,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PayloadLeaseScope {
    #[default]
    Invoke,
    Runner,
    Manual,
}

#[derive(Clone, Default)]
pub struct PayloadLeaseTable {
    leases: Arc<Mutex<BTreeMap<String, PayloadLease>>>,
}

impl PayloadLeaseTable {
    pub fn insert(
        &self,
        lease_id: impl Into<String>,
        payload: Payload,
        access: AccessMode,
        scope: PayloadLeaseScope,
    ) -> Result<WireValue, RunnerPoolError> {
        let id = lease_id.into();
        let handle = WireValue::payload_ref_from_payload(id.clone(), &payload, access);
        let lease = PayloadLease {
            id: id.clone(),
            payload,
            scope,
            created_at: Instant::now(),
            last_used: Instant::now(),
        };
        self.leases
            .lock()
            .map_err(|_| RunnerPoolError::LockPoisoned)?
            .insert(id, lease);
        Ok(handle)
    }

    pub fn resolve(&self, handle: &WirePayloadHandle) -> Result<Payload, RunnerPoolError> {
        let mut leases = self
            .leases
            .lock()
            .map_err(|_| RunnerPoolError::LockPoisoned)?;
        let lease = leases
            .get_mut(&handle.id)
            .ok_or_else(|| RunnerPoolError::MissingPayloadLease(handle.id.clone()))?;
        lease.last_used = Instant::now();
        Ok(lease.payload.clone())
    }

    pub fn release(&self, lease_id: &str) -> Result<Payload, RunnerPoolError> {
        self.leases
            .lock()
            .map_err(|_| RunnerPoolError::LockPoisoned)?
            .remove(lease_id)
            .map(|lease| lease.payload)
            .ok_or_else(|| RunnerPoolError::MissingPayloadLease(lease_id.into()))
    }

    pub fn release_scope(&self, scope: PayloadLeaseScope) -> Result<usize, RunnerPoolError> {
        let mut leases = self
            .leases
            .lock()
            .map_err(|_| RunnerPoolError::LockPoisoned)?;
        let before = leases.len();
        leases.retain(|_, lease| lease.scope != scope);
        Ok(before - leases.len())
    }

    pub fn len(&self) -> usize {
        self.leases.lock().map(|leases| leases.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
