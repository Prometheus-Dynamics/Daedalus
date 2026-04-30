use super::*;

impl PluginRegistry {
    pub(super) fn register_transport_type_decl(
        &mut self,
        key: daedalus_transport::TypeKey,
        schema: TypeExpr,
    ) -> PluginResult<()> {
        self.ensure_open()?;
        if self.transport_capabilities.type_decl(&key).is_some() {
            return Ok(());
        }
        self.transport_capabilities
            .register_type(TypeDecl::new(key).schema(schema.normalize()))
            .map_err(|source| {
                PluginError::registry("transport type capability register failed", source)
            })
    }

    fn register_explicit_transport_type_decl(
        &mut self,
        key: TypeKey,
        schema: TypeExpr,
        export: ExportPolicy,
    ) -> PluginResult<()> {
        self.ensure_open()?;
        let decl = TypeDecl::new(key.clone())
            .schema(schema.normalize())
            .export(export);
        if self.transport_capabilities.type_decl(&key).is_some()
            && self.remove_builtin_type_source(&key)
        {
            self.transport_capabilities.replace_type(decl);
            self.overridden_capabilities.types.insert(key);
            return Ok(());
        }
        self.transport_capabilities
            .register_type(decl)
            .map_err(|source| {
                PluginError::registry("transport type capability register failed", source)
            })
    }

    /// Register a native node declaration into the transport capability registry.
    pub fn register_node_decl(&mut self, decl: NodeDecl) -> PluginResult<()> {
        self.ensure_open()?;
        for port in decl.inputs.iter().chain(decl.outputs.iter()) {
            let schema = port
                .schema
                .clone()
                .unwrap_or_else(|| TypeExpr::opaque(port.type_key.to_string()));
            self.register_transport_type_decl(port.type_key.clone(), schema)?;
        }
        let id = decl.id.clone();
        if self.transport_capabilities.node_decl(&id).is_some()
            && self.remove_builtin_node_source(&id)
        {
            self.transport_capabilities.replace_node(decl);
            self.overridden_capabilities.nodes.insert(id);
            return Ok(());
        }
        self.transport_capabilities
            .register_node(decl)
            .map_err(|source| {
                PluginError::registry("transport node capability register failed", source)
            })
    }

    /// Register a transport adapter as both planner-visible metadata and executable runtime code.
    ///
    /// This is the plugin-native path for adapters: planner metadata and runtime execution are
    /// both registered through generic payload transport.
    pub fn register_transport_adapter_fn<F>(
        &mut self,
        id: impl Into<String>,
        from: TypeExpr,
        to: TypeExpr,
        f: F,
    ) -> PluginResult<()>
    where
        F: Fn(Payload, &AdaptRequest) -> Result<Payload, TransportError> + Send + Sync + 'static,
    {
        self.register_transport_adapter_fn_with_cost(id, from, to, 1, f)
    }

    /// Register a transport adapter with an explicit planner cost.
    pub fn register_transport_adapter_fn_with_cost<F>(
        &mut self,
        id: impl Into<String>,
        from: TypeExpr,
        to: TypeExpr,
        cost: u64,
        f: F,
    ) -> PluginResult<()>
    where
        F: Fn(Payload, &AdaptRequest) -> Result<Payload, TransportError> + Send + Sync + 'static,
    {
        let mut options = TransportAdapterOptions::default();
        options.cost.cpu_ns = cost.min(u64::from(u32::MAX)) as u32;
        self.register_transport_adapter_fn_with_options(id, from, to, options, f)
    }

    /// Register a transport adapter with full transport metadata.
    pub fn register_transport_adapter_fn_with_options<F>(
        &mut self,
        id: impl Into<String>,
        from: TypeExpr,
        to: TypeExpr,
        options: TransportAdapterOptions,
        f: F,
    ) -> PluginResult<()>
    where
        F: Fn(Payload, &AdaptRequest) -> Result<Payload, TransportError> + Send + Sync + 'static,
    {
        self.ensure_open()?;
        let id = id.into();
        let options = options.normalized();
        let from_for_compat = from.clone();
        let to_for_compat = to.clone();
        let adapter_id = AdapterId::new(id.clone());
        let replaces_builtin_adapter = self
            .transport_capabilities
            .adapter_decl(&adapter_id)
            .is_some()
            && self.remove_builtin_adapter_source(&adapter_id);
        if replaces_builtin_adapter {
            self.runtime_transport
                .adapters_mut()
                .replace_fn(adapter_id.clone(), f);
        } else {
            self.runtime_transport
                .register_adapter_fn(adapter_id.clone(), f)
                .map_err(|source| PluginError::TransportAdapterRegister { source })?;
        }
        let from_key = typeexpr_transport_key(&from_for_compat);
        let to_key = typeexpr_transport_key(&to_for_compat);
        self.register_transport_type_decl(from_key.clone(), from_for_compat.clone())?;
        self.register_transport_type_decl(to_key.clone(), to_for_compat.clone())?;
        let mut adapter = AdapterDecl::new(id, from_key, to_key)
            .cost(options.cost)
            .access(options.access)
            .requires_gpu(options.requires_gpu);
        if let Some(residency) = options.residency {
            adapter = adapter.residency(residency);
        }
        if let Some(layout) = options.layout {
            adapter = adapter.layout(layout);
        }
        for flag in options.feature_flags {
            adapter = adapter.feature_flag(flag);
        }
        if replaces_builtin_adapter {
            self.transport_capabilities.replace_adapter(adapter);
            self.overridden_capabilities.adapters.insert(adapter_id);
        } else {
            self.transport_capabilities
                .register_adapter(adapter)
                .map_err(|source| {
                    PluginError::registry("transport adapter capability register failed", source)
                })?;
        }
        Ok(())
    }

    /// Register a declared smart adapter.
    ///
    /// The adapter's associated metadata is installed into the capability registry and its
    /// `adapt` function is installed into the runtime adapter table under the same stable ID.
    pub fn register_smart_adapter<A: SmartAdapter>(&mut self) -> PluginResult<()> {
        self.register_transport_adapter_fn_with_options(
            A::ID,
            TypeExpr::opaque(A::FROM),
            TypeExpr::opaque(A::TO),
            A::options(),
            A::adapt,
        )
    }

    /// Register a same-type branch adapter for payloads that own their branch behavior.
    pub fn register_branch_payload_adapter<T>(
        &mut self,
        id: impl Into<String>,
        schema: TypeExpr,
    ) -> PluginResult<()>
    where
        T: BranchPayload,
    {
        let key = typeexpr_transport_key(&schema);
        let mut cost = AdaptCost::new(match T::BRANCH_KIND {
            BranchKind::Shared => AdaptKind::SharedView,
            BranchKind::Clone | BranchKind::Domain => AdaptKind::Branch,
            BranchKind::Cow => AdaptKind::Cow,
            BranchKind::Materialize => AdaptKind::Materialize,
        });
        cost.bytes_copied = match T::BRANCH_KIND {
            BranchKind::Shared => daedalus_transport::CopyCost::None,
            BranchKind::Cow => daedalus_transport::CopyCost::HeaderOnly,
            BranchKind::Clone | BranchKind::Domain | BranchKind::Materialize => {
                daedalus_transport::CopyCost::Proportional
            }
        };
        let options = TransportAdapterOptions::default()
            .cost(cost)
            .access(AccessMode::Modify);
        let from_key = key.clone();
        let to_key = key.clone();
        self.register_transport_adapter_fn_with_options(
            id,
            schema.clone(),
            schema,
            options,
            move |payload, _request| {
                let found = payload.type_key().clone();
                let value = payload
                    .get_ref::<T>()
                    .ok_or_else(|| TransportError::TypeMismatch {
                        expected: from_key.clone(),
                        found,
                    })?;
                Ok(Payload::owned(to_key.clone(), value.branch_payload()))
            },
        )
    }

    /// Register a typed CPU adapter. The input is borrowed from the payload and the output is
    /// stored as a new typed transport payload.
    pub fn register_typed_transport_adapter<S, T, F>(
        &mut self,
        id: impl Into<String>,
        from: TypeExpr,
        to: TypeExpr,
        f: F,
    ) -> PluginResult<()>
    where
        S: Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        F: Fn(&S) -> Result<T, TransportError> + Send + Sync + 'static,
    {
        self.register_typed_transport_adapter_with_cost(id, from, to, 1, f)
    }

    /// Register a typed CPU adapter with an explicit planner cost.
    pub fn register_typed_transport_adapter_with_cost<S, T, F>(
        &mut self,
        id: impl Into<String>,
        from: TypeExpr,
        to: TypeExpr,
        cost: u64,
        f: F,
    ) -> PluginResult<()>
    where
        S: Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        F: Fn(&S) -> Result<T, TransportError> + Send + Sync + 'static,
    {
        let from_key = typeexpr_transport_key(&from);
        let to_key = typeexpr_transport_key(&to);
        self.register_transport_adapter_fn_with_cost(
            id,
            from,
            to,
            cost,
            move |payload, _request| {
                let found = payload.type_key().clone();
                let Some(input) = payload.get_ref::<S>() else {
                    return Err(TransportError::TypeMismatch {
                        expected: from_key.clone(),
                        found,
                    });
                };
                f(input).map(|output| Payload::owned(to_key.clone(), output))
            },
        )
    }

    /// Register paired CPU/device upload and download adapters plus a device capability.
    pub fn register_typed_device_transport<Cpu, Device, Upload, Download>(
        &mut self,
        spec: TypedDeviceTransport,
        upload: Upload,
        download: Download,
    ) -> PluginResult<()>
    where
        Cpu: Send + Sync + 'static,
        Device: Send + Sync + 'static,
        Upload: Fn(&Cpu) -> Result<Device, TransportError> + Send + Sync + 'static,
        Download: Fn(&Device) -> Result<Cpu, TransportError> + Send + Sync + 'static,
    {
        self.ensure_open()?;
        let TypedDeviceTransport {
            device_id,
            cpu,
            device,
            upload_id,
            download_id,
        } = spec;
        let cpu_key = typeexpr_transport_key(&cpu);
        let device_key = typeexpr_transport_key(&device);

        let mut upload_options = TransportAdapterOptions::default()
            .cost(AdaptCost::device_transfer())
            .requires_gpu(true)
            .residency(Residency::Gpu);
        upload_options.access = AccessMode::Read;
        let mut download_options = TransportAdapterOptions::default()
            .cost(AdaptCost::device_transfer())
            .requires_gpu(true)
            .residency(Residency::Cpu);
        download_options.access = AccessMode::Read;

        let upload_to_key = device_key.clone();
        self.register_transport_adapter_fn_with_options(
            upload_id.clone(),
            cpu.clone(),
            device.clone(),
            upload_options,
            move |payload, _request| {
                let found = payload.type_key().clone();
                let Some(input) = payload.get_ref::<Cpu>() else {
                    return Err(TransportError::TypeMismatch {
                        expected: cpu_key.clone(),
                        found,
                    });
                };
                upload(input).map(|output| {
                    Payload::shared_with(
                        upload_to_key.clone(),
                        std::sync::Arc::new(output),
                        Residency::Gpu,
                        None,
                        payload.bytes_estimate(),
                    )
                    .with_cached_resident(payload)
                })
            },
        )?;

        let download_to_key = typeexpr_transport_key(&cpu);
        let download_from_key = device_key.clone();
        self.register_transport_adapter_fn_with_options(
            download_id.clone(),
            device.clone(),
            cpu.clone(),
            download_options,
            move |payload, _request| {
                let found = payload.type_key().clone();
                let Some(input) = payload.get_ref::<Device>() else {
                    return Err(TransportError::TypeMismatch {
                        expected: download_from_key.clone(),
                        found,
                    });
                };
                download(input).map(|output| {
                    Payload::shared_with(
                        download_to_key.clone(),
                        std::sync::Arc::new(output),
                        Residency::Cpu,
                        None,
                        payload.bytes_estimate(),
                    )
                    .with_cached_resident(payload)
                })
            },
        )?;

        let device_decl = DeviceDecl::new(
            device_id.clone(),
            typeexpr_transport_key(&cpu),
            device_key,
            upload_id,
            download_id,
        );
        if self
            .transport_capabilities
            .device_decl(&device_id)
            .is_some()
            && self.remove_builtin_device_source(&device_id)
        {
            self.transport_capabilities.replace_device(device_decl);
            self.overridden_capabilities.devices.insert(device_id);
        } else {
            self.transport_capabilities
                .register_device(device_decl)
                .map_err(|source| {
                    PluginError::registry("device capability register failed", source)
                })?;
        }
        Ok(())
    }

    pub fn register_device_transfer<D, T>(
        &mut self,
        cpu: TypeExpr,
        device: TypeExpr,
        ctx: D::Context,
    ) -> PluginResult<TypedDeviceTransport>
    where
        D: daedalus_transport::DeviceClass,
        D::Context: Clone + Send + Sync + 'static,
        T: TransferTo<D>
            + TransferFrom<D, Resident = <T as TransferTo<D>>::Resident>
            + Send
            + Sync
            + 'static,
        <T as TransferTo<D>>::Resident: Send + Sync + 'static,
    {
        let upload_id = format!("{}.upload.{}", D::ID, std::any::type_name::<T>());
        let download_id = format!("{}.download.{}", D::ID, std::any::type_name::<T>());
        let spec = TypedDeviceTransport::new(D::ID, cpu, device, upload_id, download_id);
        let upload_ctx = ctx.clone();
        let download_ctx = ctx;
        self.register_typed_device_transport::<T, <T as TransferTo<D>>::Resident, _, _>(
            spec.clone(),
            move |value| T::transfer_to(value, &upload_ctx),
            move |resident| T::transfer_from(resident, &download_ctx),
        )?;
        Ok(spec)
    }

    /// Register a named schema keyed by a stable `TypeExpr::Opaque(<key>)` string.
    pub fn register_named_type(
        &mut self,
        key: impl Into<String>,
        expr: TypeExpr,
        export: HostExportPolicy,
    ) -> PluginResult<()> {
        self.ensure_open()?;
        let key = key.into();
        daedalus_data::named_types::register_named_type(key.clone(), expr.clone(), export)
            .map_err(|message| PluginError::NamedType { message })?;
        self.register_explicit_transport_type_decl(
            TypeKey::new(key),
            expr,
            host_export_policy_to_transport(export),
        )
    }

    pub fn register_boundary_contract(
        &mut self,
        contract: BoundaryTypeContract,
    ) -> PluginResult<()> {
        self.ensure_open()?;
        if let Some(existing) = self.boundary_contracts.get(&contract.type_key) {
            existing.compatible_with(&contract)?;
        }
        daedalus_transport::register_boundary_contract(contract.clone());
        self.boundary_contracts
            .insert(contract.type_key.clone(), contract);
        Ok(())
    }

    pub fn boundary_contract(&self, type_key: &TypeKey) -> Option<&BoundaryTypeContract> {
        self.boundary_contracts.get(type_key)
    }

    /// Register a stable, Daedalus-facing schema identity for a Rust type.
    ///
    /// This links the Rust runtime type `T` to `TypeExpr::Opaque(T::TYPE_KEY)` for port typing,
    /// and registers the richer schema (`T::type_expr()`) for UI/tooling.
    pub fn register_daedalus_type<T: DaedalusTypeExpr>(
        &mut self,
        export: HostExportPolicy,
    ) -> PluginResult<()> {
        let expr = TypeExpr::Opaque(T::TYPE_KEY.to_string());
        self.type_registry.register_type::<T>(expr.clone());
        daedalus_data::typing::register_type::<T>(expr);
        self.register_named_type(T::TYPE_KEY, T::type_expr(), export)
    }

    /// Register a stable schema identity *and* a `ToValue` serializer for host-visible transport.
    pub fn register_daedalus_value<T>(&mut self) -> PluginResult<()>
    where
        T: DaedalusTypeExpr + ToValue + Clone + Send + Sync + 'static,
    {
        self.register_daedalus_type::<T>(HostExportPolicy::Value)?;
        self.register_value_serializer::<T, _>(|v| v.to_value());
        Ok(())
    }

    /// Register a host-bridge value serializer for `T` using `ToValue`.
    ///
    /// Useful for container types like `Vec<T>` where you don't want a separate named type key.
    pub fn register_to_value_serializer<T>(&mut self)
    where
        T: ToValue + Clone + Send + Sync + 'static,
    {
        self.register_value_serializer::<T, _>(|v| v.to_value());
    }

    /// Register a conversion for constant default values.
    ///
    /// This is the preferred API for dynamic plugins so the host and plugin share a single
    /// coercer map stored in the host-owned `PluginRegistry`.
    pub fn register_const_coercer<T, F>(&mut self, coercer: F)
    where
        T: Any + Send + Sync + 'static,
        F: Fn(&daedalus_data::model::Value) -> Option<T> + Send + Sync + 'static,
    {
        if self.ensure_open().is_err() {
            return;
        }
        let key = std::any::type_name::<T>();
        let mut guard = self
            .const_coercers
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.insert(
            key,
            Box::new(move |v| coercer(v).map(|t| Box::new(t) as Box<dyn Any + Send + Sync>)),
        );
    }

    /// Register a serializer for outbound host-bridge values.
    ///
    /// This enables host-bridge serialization for plugin-defined structured payload types by
    /// converting them into `daedalus_data::model::Value`.
    pub fn register_value_serializer<T, F>(&mut self, serializer: F)
    where
        T: Any + Clone + Send + Sync + 'static,
        F: Fn(&T) -> daedalus_data::model::Value + Send + Sync + 'static,
    {
        if self.ensure_open().is_err() {
            return;
        }
        crate::host_bridge::register_value_serializer_in::<T, F>(
            &self.value_serializers,
            serializer,
        );
    }

    /// Register an enum type for UI/typing and enable constant binding for it.
    ///
    /// This lets node function signatures take a strongly-typed enum (e.g. `mode: ExecMode`)
    /// while allowing JSON-authored graphs to provide the value as either:
    /// - `Value::Int(2)` (index into the registered variant list)
    /// - `Value::String("cpu")` (variant name)
    /// - `Value::Enum { name: "cpu", .. }` (variant name)
    ///
    /// The enum `T` must be `DeserializeOwned` so we can construct it from the variant name.
    pub fn register_enum<T>(&mut self, variants: impl IntoIterator<Item = impl Into<String>>)
    where
        T: Any + Send + Sync + 'static + DeserializeOwned,
    {
        if self.ensure_open().is_err() {
            return;
        }
        let variants =
            std::sync::Arc::new(variants.into_iter().map(Into::into).collect::<Vec<_>>());

        fn resolve_enum_name(variants: &[String], raw: &str) -> Option<String> {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return None;
            }
            variants
                .iter()
                .find(|name| name.eq_ignore_ascii_case(trimmed))
                .cloned()
        }

        fn resolve_enum_name_from_index(variants: &[String], idx: i64) -> Option<String> {
            if idx < 0 {
                return None;
            }
            variants.get(idx as usize).cloned()
        }

        let direct_variants = variants.clone();
        self.register_const_coercer::<T, _>(move |v| {
            let name = match v {
                daedalus_data::model::Value::Int(i) => {
                    resolve_enum_name_from_index(&direct_variants, *i)
                }
                daedalus_data::model::Value::String(s) => {
                    resolve_enum_name(&direct_variants, s.as_ref())
                }
                daedalus_data::model::Value::Enum(ev) => {
                    resolve_enum_name(&direct_variants, &ev.name)
                }
                _ => None,
            }?;
            serde_json::from_value::<T>(serde_json::Value::String(name)).ok()
        });

        // Optional enum inputs are common in node signatures (e.g. `mode: Option<ExecMode>`).
        // Register a dedicated coercer so const/default values can bind directly without each
        // plugin having to duplicate Option<T> registration glue.
        let optional_variants = variants.clone();
        self.register_const_coercer::<Option<T>, _>(move |v| {
            let name = match v {
                daedalus_data::model::Value::Int(i) => {
                    resolve_enum_name_from_index(&optional_variants, *i)
                }
                daedalus_data::model::Value::String(s) => {
                    resolve_enum_name(&optional_variants, s.as_ref())
                }
                daedalus_data::model::Value::Enum(ev) => {
                    resolve_enum_name(&optional_variants, &ev.name)
                }
                _ => None,
            }?;
            serde_json::from_value::<T>(serde_json::Value::String(name))
                .ok()
                .map(Some)
        });
    }

    /// Register a typed capability entry keyed by a string. The provided function operates
    /// on typed references; downcasting is handled internally.
    pub fn register_capability_typed<T, F>(&mut self, key: impl Into<String>, f: F)
    where
        T: Clone + Send + Sync + 'static,
        F: Fn(&T, &T) -> Result<T, crate::executor::NodeError> + Send + Sync + 'static,
    {
        if self.ensure_open().is_err() {
            return;
        }
        let key_str = key.into();
        self.capabilities.register_typed::<T, F>(key_str, f);
    }

    /// Register a typed capability entry that takes three operands of the same type.
    pub fn register_capability_typed3<T, F>(&mut self, key: impl Into<String>, f: F)
    where
        T: Clone + Send + Sync + 'static,
        F: Fn(&T, &T, &T) -> Result<T, crate::executor::NodeError> + Send + Sync + 'static,
    {
        if self.ensure_open().is_err() {
            return;
        }
        let key_str = key.into();
        self.capabilities.register_typed3::<T, F>(key_str, f);
    }
}
