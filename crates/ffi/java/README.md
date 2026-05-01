# Daedalus FFI Java

Java worker and packaging integration target for Daedalus FFI plugins.

## Target Model

Java plugins use `BackendRuntimeModel::PersistentWorker`: start one JVM, load the
classpath once, negotiate the worker protocol, advertise supported node ids, and dispatch repeated
method calls.

## Package Shape

Java packages should emit or lower into:

- `PluginSchema` for node and port shape
- per-node `BackendConfig` with `backend = java`, `runtime_model = persistent_worker`,
  `entry_class`, `entry_symbol`, `classpath`, `native_library_paths`, and `executable`
- `PluginPackage` artifacts for jars/classes directories under `_bundle/java/`
- native libraries under `_bundle/native/<platform>/`
- optional Maven coordinate and Gradle project metadata for reproducibility

`java_worker_launch` builds the `-cp` and `java.library.path` arguments from `BackendConfig`; package
builders should rewrite classpath and native library paths to bundled paths before launch.

## Current Status

This crate owns Java packaging helpers for classpath entries, jars/classes directories, Maven/Gradle
metadata, native library metadata, worker launch args, and persistent worker dispatch.
