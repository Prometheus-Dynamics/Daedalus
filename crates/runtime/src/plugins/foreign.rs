//! Foreign interfaces in the registry: owner providers, which become zero-copy `View` adapters
//! from the owner's key to the interface key, and the interfaces a registry uses, which dynamic
//! plugin installation compares between host and plugin (see `daedalus_transport::foreign`).

use super::*;
use daedalus_transport::{ForeignInterface, ForeignInterfaceInfo, ForeignView, ProvideForeign};

impl PluginRegistry {
    /// Expose the owner type `O`, under its own key, through interface `I`; see
    /// [`Self::register_foreign_provider_as`].
    pub fn register_foreign_provider<O, I>(&mut self) -> PluginResult<()>
    where
        O: ProvideForeign<I> + DaedalusTypeExpr,
        I: ForeignInterface,
    {
        self.register_foreign_provider_as::<O, I>(O::TYPE_KEY)
    }

    /// Expose payloads of `O` under `owner_key` through interface `I`.
    ///
    /// Registers a `View` adapter (`daedalus.foreign:<owner_key>-><I::KEY>`) that retypes the
    /// payload under the interface key and attaches `O`'s provider
    /// ([`Payload::provide_foreign`]): no copy, no allocation, same storage, residency and
    /// lineage. The node's `FrameView<'_>` / `ForeignRef<'_, I>` fetch then borrows the `O`
    /// through the provider's vtable directly. The planner inserts the adapter wherever an `O`
    /// feeds a port that takes the interface. Call it in the owner's own Daedalus integration,
    /// once.
    pub fn register_foreign_provider_as<O, I>(
        &mut self,
        owner_key: impl Into<TypeKey>,
    ) -> PluginResult<()>
    where
        O: ProvideForeign<I>,
        I: ForeignInterface,
    {
        self.ensure_open()?;
        let owner_key = owner_key.into();
        self.register_foreign_interface::<I>()?;
        self.register_boundary_type::<O>(owner_key.clone())?;
        let options = TransportAdapterOptions::default()
            .cost(AdaptCost::view())
            .access(AccessMode::Read);
        let from = owner_key.clone();
        self.register_transport_adapter_fn_with_options(
            format!("daedalus.foreign:{owner_key}->{}", I::KEY),
            TypeExpr::opaque(owner_key.as_str()),
            TypeExpr::opaque(I::KEY),
            options,
            move |payload, _request| {
                payload
                    .provide_foreign::<O, I>()
                    .map_err(|payload| TransportError::type_mismatch::<O>(from.clone(), &payload))
            },
        )
    }

    /// Record that this registry uses interface `I` (providers and foreign-view ports do).
    ///
    /// Fails with [`PluginError::ForeignInterfaceConflict`] when the key is already recorded with
    /// another version or vtable layout.
    pub fn register_foreign_interface<I: ForeignInterface>(&mut self) -> PluginResult<()> {
        self.register_foreign_interface_info(*I::info())
    }

    /// [`Self::register_foreign_interface`] from an interface's identity alone (as another
    /// build, e.g. a dynamic plugin installed through its stable entry points, reports it).
    pub fn register_foreign_interface_info(
        &mut self,
        new: ForeignInterfaceInfo,
    ) -> PluginResult<()> {
        self.ensure_open()?;
        let key = TypeKey::new(new.key());
        if let Some(existing) = self.foreign_interfaces.get(&key) {
            return match existing.same_interface(&new) {
                true => Ok(()),
                false => Err(PluginError::ForeignInterfaceConflict {
                    existing: *existing,
                    new,
                }),
            };
        }
        self.register_transport_type_decl(key.clone(), TypeExpr::opaque(new.key()))?;
        self.foreign_interfaces.insert(key, new);
        Ok(())
    }

    /// The foreign interfaces this registry uses, by key (see
    /// [`Self::register_foreign_interface`]).
    pub fn foreign_interfaces(&self) -> &BTreeMap<TypeKey, ForeignInterfaceInfo> {
        &self.foreign_interfaces
    }

    /// Macro support: a node input of foreign view type `V` (`FrameView<'_>`, ...).
    #[doc(hidden)]
    pub fn register_foreign_port<'a, V: ForeignView<'a>>(&mut self) -> PluginResult<()> {
        self.register_foreign_interface::<V::Interface>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::portable::Arc;
    use daedalus_transport::{FrameInterface, FramePlane, FrameResidency, FrameSource};

    struct Gray(Vec<u8>);

    impl FrameSource for Gray {
        fn width(&self) -> u32 {
            self.0.len() as u32
        }
        fn height(&self) -> u32 {
            1
        }
        fn format(&self) -> u32 {
            daedalus_transport::fourcc(b"R8  ")
        }
        fn residency(&self) -> FrameResidency {
            FrameResidency::Cpu
        }
        fn plane_count(&self) -> u32 {
            1
        }
        fn plane(&self, index: u32) -> Option<FramePlane<'_>> {
            (index == 0).then(|| FramePlane::mapped(&self.0, self.0.len() as u32))
        }
    }

    daedalus_transport::foreign_interface! {
        interface OtherFrame("daedalus:frame", version = 2);
        struct OtherFrameVTable {
            width: unsafe extern "C" fn(data: *const core::ffi::c_void) -> u32,
        }
    }

    #[test]
    fn providers_register_a_view_adapter_and_the_interface() {
        let mut registry = PluginRegistry::new();
        registry
            .register_foreign_provider_as::<Gray, FrameInterface>("test:gray")
            .unwrap();
        assert_eq!(
            registry.foreign_interfaces()[&TypeKey::new("daedalus:frame")],
            *FrameInterface::info()
        );
        let id = AdapterId::new("daedalus.foreign:test:gray->daedalus:frame");
        let decl = registry.transport_capabilities.adapter_decl(&id).unwrap();
        assert_eq!(decl.cost.kind, AdaptKind::View);

        let frame = Arc::new(Gray(vec![1, 2, 3]));
        let source =
            Payload::shared_with("test:gray", frame.clone(), Residency::External, None, None);
        let adapted = registry
            .runtime_transport
            .adapters()
            .adapt(&id, source.clone(), &AdaptRequest::new("daedalus:frame"))
            .unwrap();
        assert_eq!(adapted.type_key().as_str(), "daedalus:frame");
        assert_eq!(adapted.residency(), Residency::External);
        assert!(
            adapted.shares_storage(&source),
            "no handle payload is built"
        );
        assert!(adapted.foreign_handle().is_none());
        let view = adapted
            .foreign_borrow()
            .unwrap()
            .view::<FrameInterface>()
            .unwrap();
        assert_eq!(view.data(), Arc::as_ptr(&frame).cast());
        assert_eq!(
            view.plane(0).unwrap().data.unwrap().as_ptr(),
            frame.0.as_ptr()
        );
        let handle = adapted.to_foreign_handle().unwrap();
        assert_eq!(handle.data(), view.data());
        assert_eq!(Arc::strong_count(&frame), 3, "the handle retains the frame");
        drop(handle);

        let wrong = Payload::owned("test:gray", 7u32);
        let err = registry
            .runtime_transport
            .adapters()
            .adapt(&id, wrong, &AdaptRequest::new("daedalus:frame"))
            .unwrap_err();
        assert!(
            matches!(err, TransportError::RustTypeMismatch { .. }),
            "{err}"
        );

        let err = registry
            .register_foreign_interface::<OtherFrame>()
            .unwrap_err();
        assert!(
            matches!(err, PluginError::ForeignInterfaceConflict { .. }),
            "{err}"
        );
    }
}
