# C/C++ FFI

Status: supported (manifest + shared-library bridge).

Daedalus can load a manifest with `language: "c_cpp"` and execute nodes by `dlopen`/`dlsym` (via `libloading`) against a C ABI.

It also supports a **single-artifact** flow closer to Rust: a shared library can export its own manifest JSON via `daedalus_cpp_manifest()`, and Rust can load it with `daedalus_ffi::load_cpp_library_plugin(...)` (no `manifest.json` required).

## C ABI contract (per node)

Each node points at:

- `cc_path`: shared library path (`.so`/`.dylib`/`.dll`), relative to the manifest directory
- `cc_function`: exported C ABI symbol to call for that node
- `cc_free` (optional): exported symbol used to free returned strings (defaults to `daedalus_free`)

The node function signature is:

```c
typedef struct {
  const char* json;   // UTF-8 JSON result (malloc-allocated)
  const char* error;  // UTF-8 error (malloc-allocated) or NULL
} DaedalusCppResult;

DaedalusCppResult my_node(const char* payload_json);
void daedalus_free(char* p);
```

The `payload_json` contains the same shape used by other language bridges: `{ args, state, state_spec, ctx, node, ... }`.

## Example project

See `crates/ffi/lang/c_cpp/examples/example_project` for a copyable starter with:

- `nodes.cpp`: node implementations with a tiny JSON helper (no external deps)
- `build.sh`: compile into a shared library and emit a manifest next to it
