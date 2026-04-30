use std::ffi::CStr;
use std::fs;
use std::path::{Path, PathBuf};

use libloading::Library;
use thiserror::Error;

use crate::manifest::Manifest;

#[derive(Debug, Error)]
pub enum CppPackError {
    #[error("failed to find workspace root (Cargo.lock)")]
    WorkspaceRootNotFound,
    #[error("failed to read C/C++ manifest from dylib: {0}")]
    ReadManifest(String),
    #[error("manifest json parse error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("missing referenced file for bundling: {0}")]
    MissingFile(PathBuf),
}

#[derive(Clone, Debug)]
pub struct CppPackOptions {
    pub out_name: String,
    pub library_path: PathBuf,
    pub bundle: bool,
    pub build: bool,
}

impl Default for CppPackOptions {
    fn default() -> Self {
        Self {
            out_name: "generated_cpp_plugin".into(),
            library_path: PathBuf::new(),
            bundle: true,
            build: false,
        }
    }
}

fn find_workspace_root_from(mut cur: PathBuf) -> Result<PathBuf, CppPackError> {
    for _ in 0..12 {
        if cur.join("Cargo.lock").exists() {
            return Ok(cur);
        }
        if !cur.pop() {
            break;
        }
    }
    Err(CppPackError::WorkspaceRootNotFound)
}

fn find_workspace_root() -> Result<PathBuf, CppPackError> {
    find_workspace_root_from(std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct DaedalusCppResult {
    json: *const std::os::raw::c_char,
    error: *const std::os::raw::c_char,
}

type CppFreeFn = unsafe extern "C" fn(*mut std::os::raw::c_char);

fn read_cpp_library_manifest_json(lib_path: &Path) -> Result<String, CppPackError> {
    let lib =
        unsafe { Library::new(lib_path) }.map_err(|e| CppPackError::ReadManifest(e.to_string()))?;

    type ManifestFn = unsafe extern "C" fn() -> DaedalusCppResult;
    let mf: ManifestFn = unsafe { lib.get::<ManifestFn>(b"daedalus_cpp_manifest") }
        .map(|s| *s)
        .map_err(|e| CppPackError::ReadManifest(format!("missing daedalus_cpp_manifest: {e}")))?;
    let free_fn: CppFreeFn = unsafe { lib.get::<CppFreeFn>(b"daedalus_free") }
        .map(|s| *s)
        .map_err(|e| CppPackError::ReadManifest(format!("missing daedalus_free: {e}")))?;

    unsafe fn take(ptr: *const std::os::raw::c_char, free_fn: CppFreeFn) -> Option<String> {
        if ptr.is_null() {
            return None;
        }
        let s = unsafe { CStr::from_ptr(ptr) }.to_string_lossy().to_string();
        unsafe { free_fn(ptr as *mut std::os::raw::c_char) };
        Some(s)
    }

    let res = unsafe { mf() };
    if let Some(err) = unsafe { take(res.error, free_fn) } {
        return Err(CppPackError::ReadManifest(err));
    }
    let json = unsafe { take(res.json, free_fn) }
        .ok_or_else(|| CppPackError::ReadManifest("daedalus_cpp_manifest returned null".into()))?;
    Ok(json)
}

fn referenced_files_from_manifest(manifest: &Manifest) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for node in &manifest.nodes {
        if let Some(shader) = &node.shader {
            if let Some(p) = shader.src_path.as_deref() {
                out.push(PathBuf::from(p));
            }
            for s in &shader.shaders {
                if let Some(p) = s.src_path.as_deref() {
                    out.push(PathBuf::from(p));
                }
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

/// Generate a Rust `cdylib` wrapper example around a C/C++ dylib plugin, optionally bundling
/// the dylib + referenced files (WGSL shaders) into a self-contained artifact.
///
/// This mirrors Node/Python/Java `.pack(bundle=true)` flows: it writes:
/// - `crates/ffi/examples/{out_name}.rs` (Rust wrapper source)
/// - `crates/ffi/examples/{out_name}_bundle/` (bundle directory with copied assets)
///
/// If `build=true`, it will also `cargo build -p daedalus-ffi --example {out_name}`.
pub fn pack_cpp_library_plugin(opts: CppPackOptions) -> Result<PathBuf, CppPackError> {
    if opts.library_path.as_os_str().is_empty() {
        return Err(CppPackError::ReadManifest("missing library_path".into()));
    }
    let lib_path = opts.library_path.clone();
    let lib_dir = lib_path.parent().unwrap_or_else(|| Path::new("."));
    let lib_filename = lib_path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| CppPackError::ReadManifest("invalid library filename".into()))?
        .to_string();

    let workspace = find_workspace_root()?;
    let examples = workspace.join("crates/ffi/examples");
    fs::create_dir_all(&examples)?;
    let out_name = opts.out_name;
    let bundle_dir = examples.join(format!("{out_name}_bundle"));
    fs::create_dir_all(&bundle_dir)?;

    let manifest_json = read_cpp_library_manifest_json(&lib_path)?;
    let manifest: Manifest = serde_json::from_str(&manifest_json)?;

    // Always write a copy of the manifest for debugging/inspection.
    fs::write(bundle_dir.join("manifest.json"), manifest_json.as_bytes())?;

    let mut bundle_files: Vec<(PathBuf, PathBuf)> = Vec::new();

    // Bundle dylib at the root so `load_cpp_library_plugin` sets base_dir to the same directory
    // where shader relative paths are resolved.
    let bundled_lib = bundle_dir.join(&lib_filename);
    fs::copy(&lib_path, &bundled_lib)?;
    bundle_files.push((PathBuf::from(&lib_filename), bundled_lib.clone()));

    if opts.bundle {
        for rel in referenced_files_from_manifest(&manifest) {
            let abs = if rel.is_absolute() {
                rel.clone()
            } else {
                lib_dir.join(&rel)
            };
            if !abs.exists() {
                return Err(CppPackError::MissingFile(abs));
            }
            let dest = bundle_dir.join(&rel);
            if let Some(parent) = dest.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(&abs, &dest)?;
            bundle_files.push((rel, dest));
        }
    }

    let bundle_entries = bundle_files
        .iter()
        .map(|(rel, abs)| {
            let rel_str = rel.to_string_lossy().replace('\\', "/");
            let abs_str = abs.to_string_lossy().replace('\\', "/");
            format!(
                "    (r#\"{}\"#, include_bytes!(r#\"{}\"#) as &[u8]),",
                rel_str, abs_str
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    let wrapper = format!(
        r#"#![crate_type = "cdylib"]
use daedalus_ffi::export_plugin;
use daedalus_ffi::load_cpp_library_plugin;
use daedalus_runtime::plugins::{{Plugin, PluginInstallContext, PluginRegistry}};

fn extract_bundle() -> std::path::PathBuf {{
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("daedalus_cpp_bundle_{out_name}_{{}}_{{}}", std::process::id(), nanos));
    std::fs::create_dir_all(&dir).expect("create bundle temp dir");
    for (rel, bytes) in BUNDLE_FILES {{
        let dest = dir.join(rel);
        if let Some(parent) = dest.parent() {{
            let _ = std::fs::create_dir_all(parent);
        }}
        std::fs::write(&dest, bytes).expect("write bundled file");
    }}
    dir
}}

static BUNDLE_FILES: &[(&str, &[u8])] = &[
{bundle_entries}
];

pub struct GeneratedCppPlugin {{
    inner: daedalus_ffi::CppManifestPlugin,
}}

impl Default for GeneratedCppPlugin {{
    fn default() -> Self {{
        let base = extract_bundle();
        let lib = base.join("{lib_filename}");
        let inner = load_cpp_library_plugin(&lib).expect("load bundled cpp plugin");
        Self {{ inner }}
    }}
}}

impl Plugin for GeneratedCppPlugin {{
    fn id(&self) -> &'static str {{
        self.inner.id()
    }}

    fn install(&self, registry: &mut PluginInstallContext<'_>) -> Result<(), &'static str> {{
        self.inner.install(registry)
    }}
}}

export_plugin!(GeneratedCppPlugin);
"#
    );

    let example_rs = examples.join(format!("{out_name}.rs"));
    fs::write(&example_rs, wrapper)?;

    let profile = std::env::var("PROFILE").unwrap_or_else(|_| "debug".into());
    let artifact = workspace
        .join("target")
        .join(profile)
        .join("examples")
        .join(format!(
            "lib{out_name}.{}",
            if cfg!(target_os = "macos") {
                "dylib"
            } else if cfg!(target_os = "windows") {
                "dll"
            } else {
                "so"
            }
        ));

    if opts.build {
        let status = std::process::Command::new("cargo")
            .current_dir(&workspace)
            .args(["build", "-p", "daedalus-ffi", "--example", &out_name])
            .status()
            .map_err(CppPackError::Io)?;
        if !status.success() {
            return Err(CppPackError::ReadManifest("cargo build failed".into()));
        }
    }

    Ok(artifact)
}
