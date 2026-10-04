use super::*;
use daedalus_data::to_value::ToValue;

impl PluginRegistry {
    pub(super) fn install_standard_builtins(&mut self) -> PluginResult<()> {
        self.install_builtin_primitive_types()?;
        self.install_builtin_primitive_serializers()?;
        self.install_builtin_std_branch()?;
        self.install_builtin_numeric_widening()?;
        self.install_builtin_host_boundary()?;
        Ok(())
    }

    fn install_builtin_host_boundary(&mut self) -> PluginResult<()> {
        let mut manifest = PluginManifest::new(BUILTIN_HOST_BOUNDARY_ID);
        let host_id = NodeId::new(crate::host_bridge::HOST_BRIDGE_ID);
        let decl = NodeDecl::new(crate::host_bridge::HOST_BRIDGE_ID)
            .execution_kind(NodeExecutionKind::HostBridge);
        let decl = daedalus_planner::host_bridge_metadata()
            .into_iter()
            .fold(decl, |decl, (key, value)| decl.metadata(key, value));
        self.transport_capabilities
            .register_node(decl)
            .map_err(|source| {
                PluginError::registry("built-in host boundary node register failed", source)
            })?;
        manifest.provided_nodes.push(host_id);
        self.finish_builtin_provider(
            BUILTIN_HOST_BOUNDARY_ID,
            manifest,
            "built-in host boundary provider register failed",
        )
    }

    fn install_builtin_std_branch(&mut self) -> PluginResult<()> {
        let mut manifest = PluginManifest::new(BUILTIN_STD_BRANCH_ID);
        let registry = &mut *self;
        macro_rules! register {
            ($($ty:ty => $name:literal, $value_type:ident;)*) => {$(
                registry.register_builtin_branch_adapter::<$ty>(
                    $name,
                    TypeExpr::Scalar(ValueType::$value_type),
                    &mut manifest,
                )?;
            )*};
        }
        crate::host_bridge::for_each_builtin_primitive!(register);
        self.finish_builtin_provider(
            BUILTIN_STD_BRANCH_ID,
            manifest,
            "built-in branch provider register failed",
        )
    }

    /// Lossless numeric conversions the planner inserts implicitly: every `From` conversion
    /// between builtin numbers (`i32 -> i64`, `u32 -> i64`, `i32 -> f64`, `f32 -> f64`, ...).
    /// Narrowing and `isize`/`usize` conversions are never implicit.
    fn install_builtin_numeric_widening(&mut self) -> PluginResult<()> {
        let mut manifest = PluginManifest::new(BUILTIN_NUMERIC_WIDENING_ID);
        let registry = &mut *self;
        macro_rules! widen {
            ($($from:ident: $from_ty:ty => [$($to:ident: $to_ty:ty),*];)*) => {$($(
                registry.register_numeric_widening::<$from_ty, $to_ty>(
                    ValueType::$from,
                    ValueType::$to,
                    &mut manifest,
                )?;
            )*)*};
        }
        widen! {
            I8: i8 => [I16: i16, I32: i32, Int: i64, F32: f32, Float: f64];
            I16: i16 => [I32: i32, Int: i64, F32: f32, Float: f64];
            I32: i32 => [Int: i64, Float: f64];
            U8: u8 => [U16: u16, U32: u32, U64: u64, I16: i16, I32: i32, Int: i64, F32: f32, Float: f64];
            U16: u16 => [U32: u32, U64: u64, I32: i32, Int: i64, F32: f32, Float: f64];
            U32: u32 => [U64: u64, Int: i64, Float: f64];
            F32: f32 => [Float: f64];
        }
        self.finish_builtin_provider(
            BUILTIN_NUMERIC_WIDENING_ID,
            manifest,
            "built-in numeric widening provider register failed",
        )
    }

    /// Register the `S -> T` widening adapter. Its result is a fresh value the consumer owns,
    /// so it also serves `move`/`modify` inputs without a branch.
    fn register_numeric_widening<S, T>(
        &mut self,
        from: ValueType,
        to: ValueType,
        manifest: &mut PluginManifest,
    ) -> PluginResult<()>
    where
        S: Copy + Send + Sync + 'static,
        T: From<S> + Send + Sync + 'static,
    {
        let id = format!(
            "daedalus.builtin.widen.{}_to_{}",
            from.rust_name(),
            to.rust_name()
        );
        let (from, to) = (TypeExpr::Scalar(from), TypeExpr::Scalar(to));
        let (from_key, to_key) = (typeexpr_transport_key(&from), typeexpr_transport_key(&to));
        let options = TransportAdapterOptions::default().access(AccessMode::Modify);
        self.register_transport_adapter_fn_with_options(
            id.clone(),
            from,
            to,
            options,
            move |payload, _request| match payload.get_ref::<S>() {
                Some(value) => Ok(Payload::owned(to_key.clone(), T::from(*value))),
                None => Err(TransportError::type_mismatch::<S>(
                    from_key.clone(),
                    &payload,
                )),
            },
        )?;
        manifest.provided_adapters.push(AdapterId::new(id));
        Ok(())
    }

    fn install_builtin_primitive_types(&mut self) -> PluginResult<()> {
        let mut manifest = PluginManifest::new(BUILTIN_PRIMITIVE_TYPES_ID);
        for value_type in primitive_type_decls() {
            let schema = TypeExpr::Scalar(value_type);
            let key = typeexpr_transport_key(&schema);
            let decl = TypeDecl::new(key.clone())
                .schema(schema)
                .export(ExportPolicy::Value)
                .capability("builtin")
                .capability("primitive")
                .capability("host_value");
            self.transport_capabilities
                .register_type(decl)
                .map_err(|source| {
                    PluginError::registry("built-in primitive type register failed", source)
                })?;
            manifest.provided_types.push(key.clone());
        }
        self.finish_builtin_provider(
            BUILTIN_PRIMITIVE_TYPES_ID,
            manifest,
            "built-in primitive provider register failed",
        )
    }

    fn install_builtin_primitive_serializers(&mut self) -> PluginResult<()> {
        let registry = &mut *self;
        macro_rules! register {
            ($($ty:ty => $name:literal, $value_type:ident;)*) => {$(
                registry.register_builtin_value_serializer::<$ty>(
                    $name,
                    TypeExpr::Scalar(ValueType::$value_type),
                )?;
            )*};
        }
        crate::host_bridge::for_each_builtin_primitive!(register);

        let mut manifest = PluginManifest::new(BUILTIN_PRIMITIVE_SERIALIZERS_ID);
        for serializer in self
            .transport_capabilities
            .snapshot()
            .serializers
            .into_iter()
            .filter(|decl| decl.id.starts_with("daedalus.builtin.serializer."))
        {
            manifest.provided_serializers.push(serializer.id);
        }
        self.finish_builtin_provider(
            BUILTIN_PRIMITIVE_SERIALIZERS_ID,
            manifest,
            "built-in primitive serializer provider register failed",
        )
    }

    fn register_builtin_value_serializer<T>(
        &mut self,
        name: &str,
        schema: TypeExpr,
    ) -> PluginResult<()>
    where
        T: Any + Send + Sync + ToValue + 'static,
    {
        crate::host_bridge::register_value_serializer_in::<T, _>(
            &self.value_serializers,
            T::to_value,
        );
        let type_key = typeexpr_transport_key(&schema);
        self.register_transport_type_decl(type_key.clone(), schema)?;
        self.transport_capabilities
            .register_serializer(SerializerDecl::new(
                format!("daedalus.builtin.serializer.{name}"),
                type_key,
                ExportPolicy::Value,
            ))
            .map_err(|source| {
                PluginError::registry("built-in primitive serializer register failed", source)
            })
    }

    fn register_builtin_branch_adapter<T>(
        &mut self,
        name: &str,
        schema: TypeExpr,
        manifest: &mut PluginManifest,
    ) -> PluginResult<()>
    where
        T: BranchPayload,
    {
        let id = format!("daedalus.builtin.branch.{name}");
        self.register_branch_payload_adapter::<T>(id.clone(), schema)?;
        manifest.provided_adapters.push(AdapterId::new(id));
        Ok(())
    }

    /// Register a built-in provider manifest and record it as a built-in source.
    fn finish_builtin_provider(
        &mut self,
        provider_id: &str,
        manifest: PluginManifest,
        operation: &'static str,
    ) -> PluginResult<()> {
        let manifest = normalize_plugin_manifest(manifest);
        self.transport_capabilities
            .register_plugin(manifest.clone())
            .map_err(|source| PluginError::registry(operation, source))?;
        self.plugin_manifests
            .insert(provider_id.to_string(), manifest);
        self.provider_source_kinds
            .insert(provider_id.to_string(), CapabilitySourceKind::BuiltIn);
        Ok(())
    }
}
