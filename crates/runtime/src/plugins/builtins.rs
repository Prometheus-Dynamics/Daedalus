use super::*;
use daedalus_data::to_value::ToValue;

impl PluginRegistry {
    pub(super) fn install_standard_builtins(&mut self) -> PluginResult<()> {
        self.install_builtin_primitive_types()?;
        self.install_builtin_primitive_serializers()?;
        self.install_builtin_std_branch()?;
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
        self.register_branch_adapter_with::<T>(id.clone(), schema, branch_builtin_primitive)?;
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

/// Branch a built-in primitive of any Rust type. `i64`, `i32` and `u32` share the `Int` key (and
/// `f64`/`f32` share `Float`), so whichever branch adapter the planner picks for a key must accept
/// each of them.
fn branch_builtin_primitive(payload: &Payload, key: &TypeKey) -> Option<Payload> {
    macro_rules! branch {
        ($($ty:ty => $name:literal, $value_type:ident;)*) => {$(
            if let Some(value) = payload.get_ref::<$ty>() {
                return Some(Payload::owned(key.clone(), value.branch_payload()));
            }
        )*};
    }
    crate::host_bridge::for_each_builtin_primitive!(branch);
    None
}
