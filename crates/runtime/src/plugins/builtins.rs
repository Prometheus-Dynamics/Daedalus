use super::*;
use daedalus_core::metadata::{DYNAMIC_INPUTS_KEY, DYNAMIC_OUTPUTS_KEY};

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
            .execution_kind(NodeExecutionKind::HostBridge)
            .metadata(
                crate::host_bridge::HOST_BRIDGE_META_KEY,
                daedalus_data::model::Value::Bool(true),
            )
            .metadata(
                DYNAMIC_INPUTS_KEY,
                daedalus_data::model::Value::String(std::borrow::Cow::Borrowed("generic")),
            )
            .metadata(
                DYNAMIC_OUTPUTS_KEY,
                daedalus_data::model::Value::String(std::borrow::Cow::Borrowed("generic")),
            );
        self.transport_capabilities
            .register_node(decl)
            .map_err(|source| {
                PluginError::registry("built-in host boundary node register failed", source)
            })?;
        manifest.provided_nodes.push(host_id);
        let manifest = normalize_plugin_manifest(manifest);
        self.transport_capabilities
            .register_plugin(manifest.clone())
            .map_err(|source| {
                PluginError::registry("built-in host boundary provider register failed", source)
            })?;
        self.plugin_manifests
            .insert(BUILTIN_HOST_BOUNDARY_ID.to_string(), manifest);
        self.provider_source_kinds.insert(
            BUILTIN_HOST_BOUNDARY_ID.to_string(),
            CapabilitySourceKind::BuiltIn,
        );
        Ok(())
    }

    fn install_builtin_std_branch(&mut self) -> PluginResult<()> {
        let mut manifest = PluginManifest::new(BUILTIN_STD_BRANCH_ID);
        self.register_builtin_branch_adapter::<()>(
            "unit",
            TypeExpr::Scalar(ValueType::Unit),
            &mut manifest,
        )?;
        self.register_builtin_branch_adapter::<bool>(
            "bool",
            TypeExpr::Scalar(ValueType::Bool),
            &mut manifest,
        )?;
        self.register_builtin_branch_adapter::<i64>(
            "i64",
            TypeExpr::Scalar(ValueType::Int),
            &mut manifest,
        )?;
        self.register_builtin_branch_adapter::<i32>(
            "i32",
            TypeExpr::Scalar(ValueType::Int),
            &mut manifest,
        )?;
        self.register_builtin_branch_adapter::<u32>(
            "u32",
            TypeExpr::Scalar(ValueType::Int),
            &mut manifest,
        )?;
        self.register_builtin_branch_adapter::<f64>(
            "f64",
            TypeExpr::Scalar(ValueType::Float),
            &mut manifest,
        )?;
        self.register_builtin_branch_adapter::<f32>(
            "f32",
            TypeExpr::Scalar(ValueType::Float),
            &mut manifest,
        )?;
        self.register_builtin_branch_adapter::<String>(
            "string",
            TypeExpr::Scalar(ValueType::String),
            &mut manifest,
        )?;
        self.register_builtin_branch_adapter::<Vec<u8>>(
            "bytes",
            TypeExpr::Scalar(ValueType::Bytes),
            &mut manifest,
        )?;
        let manifest = normalize_plugin_manifest(manifest);
        self.transport_capabilities
            .register_plugin(manifest.clone())
            .map_err(|source| {
                PluginError::registry("built-in branch provider register failed", source)
            })?;
        self.plugin_manifests
            .insert(BUILTIN_STD_BRANCH_ID.to_string(), manifest);
        self.provider_source_kinds.insert(
            BUILTIN_STD_BRANCH_ID.to_string(),
            CapabilitySourceKind::BuiltIn,
        );
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
        let manifest = normalize_plugin_manifest(manifest);
        self.transport_capabilities
            .register_plugin(manifest.clone())
            .map_err(|source| {
                PluginError::registry("built-in primitive provider register failed", source)
            })?;
        self.plugin_manifests
            .insert(BUILTIN_PRIMITIVE_TYPES_ID.to_string(), manifest);
        self.provider_source_kinds.insert(
            BUILTIN_PRIMITIVE_TYPES_ID.to_string(),
            CapabilitySourceKind::BuiltIn,
        );
        Ok(())
    }

    fn install_builtin_primitive_serializers(&mut self) -> PluginResult<()> {
        self.register_builtin_value_serializer::<(), _>(
            "unit",
            TypeExpr::Scalar(ValueType::Unit),
            |v| v.to_value(),
        )?;
        self.register_builtin_value_serializer::<bool, _>(
            "bool",
            TypeExpr::Scalar(ValueType::Bool),
            |v| v.to_value(),
        )?;
        self.register_builtin_value_serializer::<i64, _>(
            "i64",
            TypeExpr::Scalar(ValueType::Int),
            |v| v.to_value(),
        )?;
        self.register_builtin_value_serializer::<i32, _>(
            "i32",
            TypeExpr::Scalar(ValueType::Int),
            |v| v.to_value(),
        )?;
        self.register_builtin_value_serializer::<u32, _>(
            "u32",
            TypeExpr::Scalar(ValueType::Int),
            |v| v.to_value(),
        )?;
        self.register_builtin_value_serializer::<f64, _>(
            "f64",
            TypeExpr::Scalar(ValueType::Float),
            |v| v.to_value(),
        )?;
        self.register_builtin_value_serializer::<f32, _>(
            "f32",
            TypeExpr::Scalar(ValueType::Float),
            |v| v.to_value(),
        )?;
        self.register_builtin_value_serializer::<String, _>(
            "string",
            TypeExpr::Scalar(ValueType::String),
            |v| v.to_value(),
        )?;
        self.register_builtin_value_serializer::<Vec<u8>, _>(
            "bytes",
            TypeExpr::Scalar(ValueType::Bytes),
            |v| v.to_value(),
        )?;

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
        let manifest = normalize_plugin_manifest(manifest);
        self.transport_capabilities
            .register_plugin(manifest.clone())
            .map_err(|source| {
                PluginError::registry(
                    "built-in primitive serializer provider register failed",
                    source,
                )
            })?;
        self.plugin_manifests
            .insert(BUILTIN_PRIMITIVE_SERIALIZERS_ID.to_string(), manifest);
        self.provider_source_kinds.insert(
            BUILTIN_PRIMITIVE_SERIALIZERS_ID.to_string(),
            CapabilitySourceKind::BuiltIn,
        );
        Ok(())
    }

    fn register_builtin_value_serializer<T, F>(
        &mut self,
        name: &str,
        schema: TypeExpr,
        serializer: F,
    ) -> PluginResult<()>
    where
        T: Any + Clone + Send + Sync + 'static,
        F: Fn(&T) -> daedalus_data::model::Value + Send + Sync + 'static,
    {
        crate::host_bridge::register_value_serializer_in::<T, F>(
            &self.value_serializers,
            serializer,
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
}
