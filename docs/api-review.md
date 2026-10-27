# Public API review (3.0.1-dev)

This review trims the public API before the 3.0.x release is sealed, so the release does not
freeze accidental surface. It covers the published crates (every workspace package without
`publish = false`).

## Method

1. Enumerate the public functions and methods from rustdoc JSON
   (`RUSTC_BOOTSTRAP=1 RUSTDOCFLAGS="-Z unstable-options --output-format json" cargo doc --workspace
   --no-deps --all-features`): each crate's public modules, `pub use` re-exports, inherent impls and
   trait declarations, skipping `#[doc(hidden)]` items. This found 2,425 distinct items.
2. For each name, search the tracked files of this repository (code, tests, examples, benches and
   the Markdown in `docs/`). 266 items have no use outside the file that defines them; only those
   are reviewed.
3. Check those 266 names against HeliOS, Styx and Eidos with `git grep` of each repository's tracked
   files. A name hit counts as a use, so the check is conservative.
4. Classify each item (below). The changes were verified with `cargo check` and `clippy
   --all-targets` for the feature sets in `scripts/ci.sh`, and `cargo doc -D warnings`.

Names are matched without type information, so a name shared by two items counts as a use of
both. The 266 are an upper bound on what is dead: the TODO estimate of about 130 used a narrower
reading.

## Decisions

| Decision | Meaning | Count |
| --- | --- | ---: |
| kept | used downstream, named in `docs/`, part of a builder or variant family (`with_*`, `try_*`, `*_async`, `*_with_*`), or a trait method or macro-generated item. Doc comments added where missing. | 125 |
| hidden | no use outside its file, but documented: `#[doc(hidden)]`, still callable (semver-public) | 61 |
| pub(crate) | no use outside its file, undocumented: `pub(crate)` | 14 |
| pub(crate), test-only | used only by unit tests in its file: `pub(crate)` behind `#[cfg(test)]` | 11 |
| removed | no caller anywhere (workspace, tests, examples, docs, downstream), undocumented | 55 |

Total: 266 items with no use outside their own file. The other 2,159 items are used outside
their own file in this repository and are unchanged.

Notes from the downstream check:

- Used downstream, so kept: `DescriptorBuilder::settable` (HeliOS), `GpuValueHandle::with_format`
  (Styx), `BufferOut::write_bytes` (HeliOS, Styx, Eidos), the bitflags helpers of `GpuBackends` and
  `GpuUsage` (`from_name`, `intersection`, `complement`, `symmetric_difference`; Styx, Eidos,
  HeliOS), `HostGraph::{last_frame_tick, frame_overhead_enabled, run_executor_once}` and
  `ExecutionDomain::load_shared_documents` (HeliOS, Eidos), `ScalarDType::bytes_per_sample` and
  `DrmFourcc::to_bytes` (Eidos, HeliOS), `HandlerRegistry::try_merge` and
  `ExecutionContext::record_metric` (Eidos), `CoreError::with_context` (HeliOS, Styx) and
  `register_all` (HeliOS).
- `GraphInput::feed_typed`, the TODO's example, is named in `docs/development.md` as the host-direct
  feed, so it is kept although no caller in this repository uses it.
- No removed, hidden or `pub(crate)` item has a downstream hit. The check ran against each
  repository's HEAD at the time of this review.
- The `daedalus-core` `channels::policy` module is removed with its three functions, which were its
  only content.

## Items

| Item | Crate | Decision | Reason |
| --- | --- | --- | --- |
| `daedalus_core::channels::async_::bounded_async` | daedalus_core | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_core::channels::async_::bounded_async_with_behavior` | daedalus_core | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_core::channels::async_::broadcast_async` | daedalus_core | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_core::channels::async_::broadcast_async_with_behavior` | daedalus_core | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_core::channels::async_::newest_async` | daedalus_core | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_core::channels::async_::newest_async_with_behavior` | daedalus_core | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_core::channels::async_::unbounded_async` | daedalus_core | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_core::channels::async_::unbounded_async_with_behavior` | daedalus_core | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_core::channels::broadcast::broadcast_with_metrics_and_behavior` | daedalus_core | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_core::channels::newest::newest_with_metrics_and_behavior` | daedalus_core | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_core::channels::policy::build_bounded` | daedalus_core | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_core::channels::policy::build_broadcast` | daedalus_core | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_core::channels::policy::build_unbounded` | daedalus_core | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_core::channels::unbounded::unbounded_with_metrics_and_behavior` | daedalus_core | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_core::clock::TickClock::default_start` | daedalus_core | pub(crate) | used only inside its own file (internal plumbing); `pub(crate)` |
| `daedalus_core::errors::CoreError::with_context` | daedalus_core | kept | used by HeliOS/Styx/Eidos (name grep) |
| `daedalus_core::metrics::InMemoryMetrics::into_sink` | daedalus_core | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_core::metrics::MetricsSink::observe_nanos` | daedalus_core | kept | trait method or generated by a macro; doc comment added |
| `daedalus_core::metrics::metrics_sink` | daedalus_core | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_data::convert::ConverterGraph::resolve_with_context_async` | daedalus_data | kept | part of a builder or fallible/async/metrics variant family |
| `daedalus_data::descriptor::DescriptorBuilder::gpu_hints` | daedalus_data | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_data::descriptor::DescriptorBuilder::settable` | daedalus_data | kept | used by HeliOS/Styx/Eidos (name grep); doc comment added |
| `daedalus_data::gpu::AsGpuHandle::as_gpu_handle` | daedalus_data | kept | trait method or generated by a macro; doc comment added |
| `daedalus_data::gpu::GpuValueHandle::with_adapter_label` | daedalus_data | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_data::gpu::GpuValueHandle::with_format` | daedalus_data | kept | used by HeliOS/Styx/Eidos (name grep); doc comment added |
| `daedalus_data::gpu::GpuValueHandle::with_kind` | daedalus_data | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_data::gpu::GpuValueHandle::with_location` | daedalus_data | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_data::json::decode_value` | daedalus_data | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_data::json::encode_value` | daedalus_data | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_data::named_types::NamedTypeRegistry::export_policy_for` | daedalus_data | pub(crate), test-only | used only by unit tests in its own file; `pub(crate)` behind `#[cfg(test)]` |
| `daedalus_data::prelude::DescriptorId::namespaced` | daedalus_data | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_data::proto::to_proto_type` | daedalus_data | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_data::schema::to_json_schema_string` | daedalus_data | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_data::schema::type_to_json_schema` | daedalus_data | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_data::typing::TypeRegistry::has_type_capability` | daedalus_data | pub(crate), test-only | used only by unit tests in its own file; `pub(crate)` behind `#[cfg(test)]` |
| `daedalus_data::typing::TypeRegistry::lookup_type_by_rust_name` | daedalus_data | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_data::typing::TypeRegistry::register_type_capabilities` | daedalus_data | pub(crate), test-only | used only by unit tests in its own file; `pub(crate)` behind `#[cfg(test)]` |
| `daedalus_data::typing::TypeRegistry::register_type_capability` | daedalus_data | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_data::typing::TypeRegistry::snapshot_by_rust_name` | daedalus_data | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_data::typing::TypeRegistry::snapshot_type_capabilities` | daedalus_data | pub(crate), test-only | used only by unit tests in its own file; `pub(crate)` behind `#[cfg(test)]` |
| `daedalus_data::typing::TypeRegistry::type_capabilities` | daedalus_data | pub(crate), test-only | used only by unit tests in its own file; `pub(crate)` behind `#[cfg(test)]` |
| `daedalus_engine::DomainGraphStats::mean_run` | daedalus_engine | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_engine::DomainStats::forward_time` | daedalus_engine | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_engine::Engine::build_runtime_plan` | daedalus_engine | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_engine::Engine::compile_plugin_registry` | daedalus_engine | kept | named in docs/ (API docs) |
| `daedalus_engine::Engine::compile_runner` | daedalus_engine | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_engine::Engine::compile_runtime_plan_with_transport` | daedalus_engine | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_engine::Engine::compile_with_transport` | daedalus_engine | kept | part of a builder or fallible/async/metrics variant family |
| `daedalus_engine::Engine::execute_scoped` | daedalus_engine | pub(crate) | used only inside its own file (internal plumbing); `pub(crate)` |
| `daedalus_engine::Engine::execute_scoped_with_plugin_registry` | daedalus_engine | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_engine::Engine::execute_scoped_with_transport` | daedalus_engine | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_engine::Engine::execute_with_host` | daedalus_engine | kept | named in docs/ (API docs); doc comment added |
| `daedalus_engine::Engine::execute_with_host_scoped` | daedalus_engine | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_engine::Engine::execute_with_plugin_registry` | daedalus_engine | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_engine::Engine::execute_with_transport` | daedalus_engine | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_engine::Engine::plan_plugin_registry` | daedalus_engine | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_engine::Engine::plan_with_config` | daedalus_engine | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_engine::Engine::run_with_patch` | daedalus_engine | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_engine::Engine::run_with_transport` | daedalus_engine | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_engine::EngineConfig::with_backpressure` | daedalus_engine | kept | named in docs/ (API docs); doc comment added |
| `daedalus_engine::EngineConfig::with_planner_gpu` | daedalus_engine | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_engine::EngineConfig::with_stream_error_policy` | daedalus_engine | kept | named in docs/ (API docs); doc comment added |
| `daedalus_engine::ExecutionDomain::has_pending_input` | daedalus_engine | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_engine::ExecutionDomain::load_shared_documents` | daedalus_engine | kept | used by HeliOS/Styx/Eidos (name grep) |
| `daedalus_engine::HostGraph::bind_payload_output` | daedalus_engine | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_engine::HostGraph::disable_frame_overhead` | daedalus_engine | kept | named in docs/ (API docs) |
| `daedalus_engine::HostGraph::frame_overhead_enabled` | daedalus_engine | kept | used by HeliOS/Styx/Eidos (name grep); doc comment added |
| `daedalus_engine::HostGraph::last_frame_tick` | daedalus_engine | kept | used by HeliOS/Styx/Eidos (name grep) |
| `daedalus_engine::HostGraph::run_executor_once` | daedalus_engine | kept | used by HeliOS/Styx/Eidos (name grep); doc comment added |
| `daedalus_engine::HostGraph::set_latest_output` | daedalus_engine | kept | named in docs/ (API docs); doc comment added |
| `daedalus_engine::diagnostics::render_error` | daedalus_engine | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_engine::diagnostics::render_plan` | daedalus_engine | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_engine::diagnostics::render_runtime` | daedalus_engine | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_ffi_core::PluginLockfile::from_package` | daedalus_ffi_core | pub(crate) | used only inside its own file (internal plumbing); `pub(crate)` |
| `daedalus_ffi_core::PluginPackage::read_descriptor` | daedalus_ffi_core | kept | no_std counterpart of from_env / feature-gated read path; doc comment added |
| `daedalus_ffi_core::ScalarDType::bytes_per_sample` | daedalus_ffi_core | kept | used by HeliOS/Styx/Eidos (name grep); doc comment added |
| `daedalus_ffi_core::package_platform_dir` | daedalus_ffi_core | pub(crate) | used only inside its own file (internal plumbing); `pub(crate)` |
| `daedalus_ffi_cpp::CppPointerLengthAbi::bytes_view` | daedalus_ffi_cpp | pub(crate), test-only | used only by unit tests in its own file; `pub(crate)` behind `#[cfg(test)]` |
| `daedalus_ffi_cpp::cpp_in_process_backend_config` | daedalus_ffi_cpp | pub(crate) | used only inside its own file (internal plumbing); `pub(crate)` |
| `daedalus_ffi_cpp::cpp_in_process_backend_config_with_pointer_abi` | daedalus_ffi_cpp | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_ffi_cpp::cpp_plugin_package` | daedalus_ffi_cpp | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_ffi_host::DecodedInvokeResponse::events_at_level` | daedalus_ffi_host | pub(crate), test-only | used only by unit tests in its own file; `pub(crate)` behind `#[cfg(test)]` |
| `daedalus_ffi_host::DecodedInvokeResponse::payload_output` | daedalus_ffi_host | kept | named in docs/ (API docs) |
| `daedalus_ffi_host::DecodedInvokeResponse::value_output` | daedalus_ffi_host | pub(crate), test-only | used only by unit tests in its own file; `pub(crate)` behind `#[cfg(test)]` |
| `daedalus_ffi_host::DecodedInvokeResponse::wire_output` | daedalus_ffi_host | pub(crate) | used only inside its own file (internal plumbing); `pub(crate)` |
| `daedalus_ffi_host::HostInstallPlan::from_schema` | daedalus_ffi_host | pub(crate) | used only inside its own file (internal plumbing); `pub(crate)` |
| `daedalus_ffi_host::RunnerPool::ffi_telemetry` | daedalus_ffi_host | removed | dead: only referenced from its own file, which no longer needs it |
| `daedalus_ffi_host::RunnerPool::set_ffi_telemetry` | daedalus_ffi_host | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_ffi_node::node_plugin_package` | daedalus_ffi_node | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_ffi_node::node_worker_backend_config` | daedalus_ffi_node | pub(crate) | used only inside its own file (internal plumbing); `pub(crate)` |
| `daedalus_ffi_node::node_worker_backend_config_with_transport` | daedalus_ffi_node | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_ffi_python::python_plugin_package` | daedalus_ffi_python | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_ffi_python::python_worker_backend_config` | daedalus_ffi_python | pub(crate) | used only inside its own file (internal plumbing); `pub(crate)` |
| `daedalus_ffi_python::python_worker_backend_config_with_transport` | daedalus_ffi_python | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_gpu::AcquireFenceWait::has_timeout` | daedalus_gpu | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_gpu::Backing::shared_arc` | daedalus_gpu | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_gpu::Compute::as_cpu` | daedalus_gpu | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_gpu::Compute::as_gpu` | daedalus_gpu | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_gpu::Compute::into_gpu` | daedalus_gpu | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_gpu::Compute::is_gpu` | daedalus_gpu | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_gpu::DrmFourcc::to_bytes` | daedalus_gpu | kept | used by HeliOS/Styx/Eidos (name grep) |
| `daedalus_gpu::GpuAsyncBackend::create_image_async` | daedalus_gpu | kept | trait method or generated by a macro; doc comment added |
| `daedalus_gpu::GpuBackends::complement` | daedalus_gpu | kept | trait method or generated by a macro |
| `daedalus_gpu::GpuBackends::from_bits_retain` | daedalus_gpu | kept | trait method or generated by a macro |
| `daedalus_gpu::GpuBackends::from_name` | daedalus_gpu | kept | trait method or generated by a macro |
| `daedalus_gpu::GpuBackends::intersection` | daedalus_gpu | kept | trait method or generated by a macro |
| `daedalus_gpu::GpuBackends::is_all` | daedalus_gpu | kept | trait method or generated by a macro |
| `daedalus_gpu::GpuBackends::iter_names` | daedalus_gpu | kept | trait method or generated by a macro |
| `daedalus_gpu::GpuBackends::symmetric_difference` | daedalus_gpu | kept | trait method or generated by a macro |
| `daedalus_gpu::GpuBlockInfo::for_format` | daedalus_gpu | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_gpu::GpuContextHandle::alloc_download_buffer` | daedalus_gpu | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_gpu::GpuContextHandle::alloc_upload_buffer` | daedalus_gpu | pub(crate) | used only inside its own file (internal plumbing); `pub(crate)` |
| `daedalus_gpu::GpuContextHandle::skipped_summary` | daedalus_gpu | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_gpu::GpuUsage::complement` | daedalus_gpu | kept | trait method or generated by a macro |
| `daedalus_gpu::GpuUsage::from_bits_retain` | daedalus_gpu | kept | trait method or generated by a macro |
| `daedalus_gpu::GpuUsage::from_name` | daedalus_gpu | kept | trait method or generated by a macro |
| `daedalus_gpu::GpuUsage::intersection` | daedalus_gpu | kept | trait method or generated by a macro |
| `daedalus_gpu::GpuUsage::is_all` | daedalus_gpu | kept | trait method or generated by a macro |
| `daedalus_gpu::GpuUsage::iter_names` | daedalus_gpu | kept | trait method or generated by a macro |
| `daedalus_gpu::GpuUsage::symmetric_difference` | daedalus_gpu | kept | trait method or generated by a macro |
| `daedalus_gpu::WgpuBackend::new_with_staging_pool_config_async` | daedalus_gpu | kept | named in docs/ (API docs); doc comment added |
| `daedalus_gpu::active_backend` | daedalus_gpu | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_gpu::shader::BindingSpec::storage_texture` | daedalus_gpu | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_gpu::shader::BufferOut::write_bytes` | daedalus_gpu | kept | used by HeliOS/Styx/Eidos (name grep); doc comment added |
| `daedalus_gpu::shader::BufferOut::write_bytes_no_readback` | daedalus_gpu | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_gpu::shader::GpuState::new_stateful` | daedalus_gpu | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_gpu::shader::GpuState::new_stateful_with_gpu` | daedalus_gpu | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_gpu::shader::GpuState::new_with_gpu` | daedalus_gpu | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_gpu::shader::GpuState::read_async` | daedalus_gpu | kept | named in docs/ (API docs) |
| `daedalus_gpu::shader::SamplerDesc::with_address_mode` | daedalus_gpu | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_gpu::shader::SamplerDesc::with_filters` | daedalus_gpu | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_gpu::shader::SamplerDesc::with_mipmap_filter` | daedalus_gpu | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_gpu::shader::ShaderContext::dispatch_bindings_async` | daedalus_gpu | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_gpu::shader::ShaderContext::dispatch_by_name` | daedalus_gpu | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_gpu::shader::ShaderContext::dispatch_by_name_async` | daedalus_gpu | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_gpu::shader::ShaderContext::dispatch_first` | daedalus_gpu | kept | used by HeliOS/Styx/Eidos (name grep) |
| `daedalus_gpu::shader::ShaderContext::dispatch_first_async` | daedalus_gpu | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_gpu::shader::ShaderRunOutput::into_payload_with_ctx` | daedalus_gpu | kept | part of a builder or fallible/async/metrics variant family |
| `daedalus_gpu::shader::ShaderRunOutput::into_texture_rgba8` | daedalus_gpu | kept | named in docs/ (API docs) |
| `daedalus_gpu::shader::ShaderRunOutput::storage_r32f` | daedalus_gpu | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_gpu::shader::ShaderRunOutput::storage_rgba16f_image` | daedalus_gpu | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_gpu::shader::ShaderRunOutput::storage_rgba8_image` | daedalus_gpu | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_gpu::shader::ShaderRunOutput::texture_handle` | daedalus_gpu | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_gpu::shader::ShaderRunOutput::texture_r32f` | daedalus_gpu | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_gpu::shader::ShaderRunOutput::texture_r32f_gray_u8` | daedalus_gpu | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_gpu::shader::ShaderRunOutput::texture_r32f_image` | daedalus_gpu | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_gpu::shader::ShaderRunOutput::texture_rgba16f` | daedalus_gpu | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_gpu::shader::ShaderRunOutput::texture_rgba16f_image` | daedalus_gpu | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_gpu::shader::ShaderRunOutput::texture_rgba8` | daedalus_gpu | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_gpu::shader::ShaderRunOutput::texture_rgba8_image` | daedalus_gpu | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_gpu::shader::TextureOut::from_input` | daedalus_gpu | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_gpu::shader::TextureOut::from_input_ctx` | daedalus_gpu | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_gpu::shader::TextureOut::write_with_readback` | daedalus_gpu | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_gpu::shader::dispatch_shader_with_options` | daedalus_gpu | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_gpu::shader::dispatch_shader_with_options_async` | daedalus_gpu | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_gpu::shader::max_inflight_submissions_per_device` | daedalus_gpu | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_gpu::shader::set_max_inflight_submissions_per_device` | daedalus_gpu | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_gpu::upload_bytes` | daedalus_gpu | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_mcu_build::plan_specs` | daedalus_mcu_build | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_mcu_build::write_out_dir` | daedalus_mcu_build | kept | named in docs/ (API docs) |
| `daedalus_nodes::bundle_demo::DemoPlugin::node_decode` | daedalus_nodes | kept | trait method or generated by a macro |
| `daedalus_nodes::bundle_demo::DemoPlugin::node_frame_src` | daedalus_nodes | kept | trait method or generated by a macro |
| `daedalus_nodes::bundle_demo::DemoPlugin::node_sink` | daedalus_nodes | kept | trait method or generated by a macro |
| `daedalus_nodes::bundle_demo::install_bundle` | daedalus_nodes | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_nodes::register_all` | daedalus_nodes | kept | used by HeliOS/Styx/Eidos (name grep) |
| `daedalus_planner::PluginRequirement::check_version` | daedalus_planner | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_registry::prelude::CapabilityRegistry::replace_serializer` | daedalus_registry | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_registry::prelude::CapabilityRegistry::serializer_decl` | daedalus_registry | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_registry::prelude::NodeExecutionKind::from_metadata_value` | daedalus_registry | pub(crate) | used only inside its own file (internal plumbing); `pub(crate)` |
| `daedalus_registry::prelude::NodeExecutionKind::write_to_metadata` | daedalus_registry | pub(crate), test-only | used only by unit tests in its own file; `pub(crate)` behind `#[cfg(test)]` |
| `daedalus_registry::prelude::NodeFire::write_to_metadata` | daedalus_registry | removed | dead: only referenced from its own file, which no longer needs it |
| `daedalus_runtime::DemandSlice::active_edge_count` | daedalus_runtime | pub(crate) | used only inside its own file (internal plumbing); `pub(crate)` |
| `daedalus_runtime::DemandSlice::active_node_count` | daedalus_runtime | pub(crate) | used only inside its own file (internal plumbing); `pub(crate)` |
| `daedalus_runtime::ExecutionContext::record_frame_scratch_bytes` | daedalus_runtime | removed | dead: only referenced from its own file, which no longer needs it |
| `daedalus_runtime::ExecutionContext::record_metric` | daedalus_runtime | kept | used by HeliOS/Styx/Eidos (name grep); doc comment added |
| `daedalus_runtime::ExecutionContext::record_warm_cache_bytes` | daedalus_runtime | removed | dead: only referenced from its own file, which no longer needs it |
| `daedalus_runtime::ExecutionContext::with_frame_scratch` | daedalus_runtime | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_runtime::ExecutionContext::with_persistent_bytes` | daedalus_runtime | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_runtime::ExecutionContext::with_warm_cache_bytes` | daedalus_runtime | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_runtime::ExecutionTelemetry::is_lifecycle_enabled` | daedalus_runtime | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_runtime::ExecutionTelemetry::record_edge_gpu_transfer` | daedalus_runtime | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_runtime::FrameOverheadWindow::latest_mut` | daedalus_runtime | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_runtime::GraphInput::feed_typed` | daedalus_runtime | kept | named in docs/ (API docs); doc comment added |
| `daedalus_runtime::GraphOutput::try_recv_typed` | daedalus_runtime | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_runtime::HostBridgeHandle::drain_arcs` | daedalus_runtime | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_runtime::HostBridgeHandle::try_pop_arc` | daedalus_runtime | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_runtime::ManagedByteBuffer::clear_live` | daedalus_runtime | pub(crate) | used only inside its own file (internal plumbing); `pub(crate)` |
| `daedalus_runtime::MetricsLevel::is_hardware` | daedalus_runtime | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_runtime::NodeHandle::alias_id` | daedalus_runtime | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_runtime::NodeIo::from_single_input` | daedalus_runtime | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_runtime::NodeIo::get_typed_mut` | daedalus_runtime | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_runtime::NodeIo::push_arc_as_default` | daedalus_runtime | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_runtime::NodeIo::push_as_default` | daedalus_runtime | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_runtime::NodeIo::push_payload_default` | daedalus_runtime | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_runtime::NodeIo::with_output_ports` | daedalus_runtime | kept | part of a builder or fallible/async/metrics variant family |
| `daedalus_runtime::PayloadInspection::to_display_value` | daedalus_runtime | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_runtime::PayloadInspection::to_json_with` | daedalus_runtime | kept | named in docs/ (API docs) |
| `daedalus_runtime::PortHandle::node_alias_id` | daedalus_runtime | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_runtime::PortHandle::port_id` | daedalus_runtime | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_runtime::Profiler::export_json` | daedalus_runtime | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_runtime::Profiler::telemetry_mut` | daedalus_runtime | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_runtime::RuntimeEdgePolicy::is_latest_only` | daedalus_runtime | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_runtime::RuntimePlan::active_nodes_for_sinks` | daedalus_runtime | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_runtime::RuntimeResources::record_frame_scratch_bytes` | daedalus_runtime | removed | dead: only referenced from its own file, which no longer needs it |
| `daedalus_runtime::RuntimeResources::record_warm_cache_bytes` | daedalus_runtime | removed | dead: only referenced from its own file, which no longer needs it |
| `daedalus_runtime::RuntimeResources::with_frame_scratch` | daedalus_runtime | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_runtime::RuntimeResources::with_persistent_bytes` | daedalus_runtime | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_runtime::RuntimeResources::with_warm_cache_bytes` | daedalus_runtime | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_runtime::StateStore::get_checked` | daedalus_runtime | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_runtime::StateStore::set_typed` | daedalus_runtime | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_runtime::StreamGraph::export_profile_json` | daedalus_runtime | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_runtime::StreamGraph::host_events` | daedalus_runtime | kept | named in docs/ (API docs); doc comment added |
| `daedalus_runtime::StreamGraph::host_stats` | daedalus_runtime | kept | named in docs/ (API docs); doc comment added |
| `daedalus_runtime::StreamGraph::map_executor` | daedalus_runtime | kept | named in docs/ (API docs) |
| `daedalus_runtime::StreamGraph::profiler_snapshot` | daedalus_runtime | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_runtime::StreamGraph::with_host_alias` | daedalus_runtime | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_runtime::alloc_probe::current_scope` | daedalus_runtime | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_runtime::config::RuntimeDebugConfig::from_lookup` | daedalus_runtime | kept | no_std counterpart of from_env / feature-gated read path; doc comment added |
| `daedalus_runtime::config::RuntimeDebugConfig::from_lookup_with_diagnostics` | daedalus_runtime | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_runtime::const_coerce::deserialize_value` | daedalus_runtime | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_runtime::graph_builder::GraphBuilder::connect_from_nested` | daedalus_runtime | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_runtime::graph_builder::GraphBuilder::connect_handles` | daedalus_runtime | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_runtime::graph_builder::GraphBuilder::connect_ports` | daedalus_runtime | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_runtime::graph_builder::GraphBuilder::connect_to_nested` | daedalus_runtime | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_runtime::graph_builder::GraphBuilder::edge_metadata` | daedalus_runtime | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_runtime::graph_builder::GraphBuilder::node_metadata_map` | daedalus_runtime | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_runtime::graph_builder::GraphBuilder::node_metadata_map_by_id` | daedalus_runtime | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_runtime::graph_builder::GraphBuilder::node_port` | daedalus_runtime | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_runtime::graph_builder::GraphBuilder::output_as` | daedalus_runtime | kept | named in docs/ (API docs) |
| `daedalus_runtime::graph_builder::GraphBuilder::single_node_io` | daedalus_runtime | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_runtime::graph_builder::GraphBuilder::single_node_ports` | daedalus_runtime | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_runtime::graph_builder::GraphBuilder::try_connect_with_metadata` | daedalus_runtime | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_runtime::graph_builder::GraphBuilder::try_edge_metadata` | daedalus_runtime | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_runtime::graph_builder::GraphBuilder::try_node_spec` | daedalus_runtime | kept | part of a builder or fallible/async/metrics variant family |
| `daedalus_runtime::graph_builder::GraphCtx::node_as` | daedalus_runtime | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_runtime::graph_builder::GraphScope::add_handle_with_compute` | daedalus_runtime | kept | used by HeliOS/Styx/Eidos (name grep) |
| `daedalus_runtime::graph_builder::GraphScope::connect_policy` | daedalus_runtime | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_runtime::graph_builder::GraphScope::try_add_handle` | daedalus_runtime | kept | part of a builder or fallible/async/metrics variant family |
| `daedalus_runtime::graph_builder::GraphScope::try_add_handle_with_compute` | daedalus_runtime | kept | part of a builder or fallible/async/metrics variant family |
| `daedalus_runtime::graph_builder::GraphScope::try_connect_policy` | daedalus_runtime | kept | part of a builder or fallible/async/metrics variant family |
| `daedalus_runtime::handler_registry::HandlerRegistry::try_merge` | daedalus_runtime | kept | used by HeliOS/Styx/Eidos (name grep); doc comment added |
| `daedalus_runtime::handler_registry::HandlerRegistry::try_on_direct_payload` | daedalus_runtime | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_runtime::handler_registry::HandlerRegistry::try_on_stateful` | daedalus_runtime | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_runtime::handler_registry::HandlerRegistry::try_with_prefix` | daedalus_runtime | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_runtime::host_bridge::multicam::source_sequence` | daedalus_runtime | kept | named in docs/ (API docs) |
| `daedalus_runtime::io::CowArcMut::as_arc` | daedalus_runtime | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_runtime::plugins::PluginInstallContext::manifest_mut` | daedalus_runtime | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_runtime::plugins::PluginRegistry::install_and_freeze` | daedalus_runtime | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_runtime::plugins::PluginRegistry::register_smart_adapter` | daedalus_runtime | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_runtime::plugins::PluginRegistry::register_transport_adapter_fn_with_cost` | daedalus_runtime | kept | part of a builder or fallible/async/metrics variant family |
| `daedalus_runtime::plugins::PluginRegistry::register_typed_transport_adapter` | daedalus_runtime | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
| `daedalus_runtime::plugins::PluginRegistry::register_typed_transport_adapter_with_cost` | daedalus_runtime | kept | part of a builder or fallible/async/metrics variant family |
| `daedalus_runtime::plugins::PluginRegistry::take_runtime_transport` | daedalus_runtime | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_runtime::plugins::PluginRegistry::take_transport_capabilities` | daedalus_runtime | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_transport::BoundaryStorage::is_taken` | daedalus_transport | pub(crate), test-only | used only by unit tests in its own file; `pub(crate)` behind `#[cfg(test)]` |
| `daedalus_transport::BranchPayload::estimated_branch_bytes` | daedalus_transport | kept | trait method or generated by a macro; doc comment added |
| `daedalus_transport::Payload::boundary_shared` | daedalus_transport | pub(crate), test-only | used only by unit tests in its own file; `pub(crate)` behind `#[cfg(test)]` |
| `daedalus_transport::Payload::cache_current` | daedalus_transport | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_transport::Payload::insert_cached_resident` | daedalus_transport | kept | named in docs/ (API docs); doc comment added |
| `daedalus_transport::Payload::resident_arc` | daedalus_transport | removed | no caller anywhere in the workspace or the downstream repos, undocumented |
| `daedalus_transport::Payload::try_borrow_boundary_mut` | daedalus_transport | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_transport::Payload::try_borrow_boundary_ref` | daedalus_transport | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_transport::Payload::try_take_boundary_owned` | daedalus_transport | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_transport::PayloadLineage::with_parent` | daedalus_transport | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_transport::PayloadLineage::with_source_timestamp` | daedalus_transport | kept | part of a builder or fallible/async/metrics variant family; doc comment added |
| `daedalus_transport::PressurePolicy::is_single_slot` | daedalus_transport | hidden | no caller outside its file; documented, so hidden (`#[doc(hidden)]`) rather than removed |
