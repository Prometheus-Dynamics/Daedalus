use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use daedalus_ffi::{CppPackOptions, pack_cpp_library_plugin};

fn workspace_root() -> PathBuf {
    let mut root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    root.pop(); // crates/ffi
    root.pop(); // crates
    root
}

fn temp_dir(prefix: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "daedalus_cpp_pack_bundle_suite_{prefix}_{nanos}_{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn has_cmd(cmd: &str, arg: &str) -> bool {
    Command::new(cmd).arg(arg).output().is_ok()
}

fn dylib_ext() -> &'static str {
    if cfg!(target_os = "windows") {
        "dll"
    } else if cfg!(target_os = "macos") {
        "dylib"
    } else {
        "so"
    }
}

fn compile_cpp_lib_with_shader(out_dir: &Path, lib_stem: &str, plugin_name: &str) -> PathBuf {
    let cxx = std::env::var("CXX").unwrap_or_else(|_| "c++".to_string());
    if !has_cmd(&cxx, "--version") {
        eprintln!("skipping: C++ compiler not found (set CXX or install c++)");
        return PathBuf::new();
    }

    let hdr_dir = workspace_root().join("crates/ffi/lang/c_cpp/sdk");
    let src = out_dir.join(format!("{lib_stem}.cpp"));
    let lib = out_dir.join(format!("lib{lib_stem}.{}", dylib_ext()));
    let shader_dir = out_dir.join("shaders");
    fs::create_dir_all(&shader_dir).expect("create shader dir");
    fs::write(
        shader_dir.join("write_u32.wgsl"),
        "@compute @workgroup_size(1,1,1)\nfn main() {}\n",
    )
    .expect("write shader");

    let code = format!(
        r#"#include "daedalus.hpp"
#include <cstdint>
#include <tuple>

static int32_t add_i32(int32_t a, int32_t b) {{ return a + b; }}

DAEDALUS_NODE("demo_cpp:add", add_i32, DAEDALUS_PORTS(a,b), DAEDALUS_PORTS(out))
DAEDALUS_REGISTER_SHADER_NODE_T(demo_cpp_shader_write_u32,
                                "demo_cpp:shader_write_u32",
                                DAEDALUS_NAMES(),
                                std::tuple<>{{}},
                                DAEDALUS_PORTS(out),
                                std::tuple<uint32_t>{{}},
                                daedalus::shader().file("shaders/write_u32.wgsl").shader_name("write_u32").invocations(1,1,1).storage_u32_rw(0, "out", 4, true))

DAEDALUS_PLUGIN("{plugin_name}", "1.0.0", "cpp pack/bundle test plugin")
"#
    );
    fs::write(&src, code).expect("write cpp source");

    let status = Command::new(&cxx)
        .current_dir(out_dir)
        .args([
            "-std=c++17",
            "-O2",
            "-fPIC",
            "-shared",
            &format!("-I{}", hdr_dir.display()),
            src.to_string_lossy().as_ref(),
            "-o",
            lib.to_string_lossy().as_ref(),
        ])
        .status()
        .expect("compile c++ dylib");
    assert!(status.success(), "c++ compile failed");
    lib
}

#[test]
fn cpp_pack_bundle_produces_self_contained_bundle() {
    if std::env::var_os("DAEDALUS_TEST_CPP_BUNDLE").is_none() {
        eprintln!("DAEDALUS_TEST_CPP_BUNDLE not set; skipping");
        return;
    }

    let out_dir = temp_dir("bundle");
    let lib_path = compile_cpp_lib_with_shader(&out_dir, "cpp_pack_nodes", "demo_cpp_pack");
    if lib_path.as_os_str().is_empty() {
        return;
    }

    let out_name = format!("cpp_pack_bundle_{}", std::process::id());
    let _artifact = pack_cpp_library_plugin(CppPackOptions {
        out_name: out_name.clone(),
        library_path: lib_path.clone(),
        bundle: true,
        build: false,
    })
    .expect("pack c++ library plugin");

    let examples = workspace_root().join("crates/ffi/examples");
    let bundle_dir = examples.join(format!("{out_name}_bundle"));
    let bundled_manifest = bundle_dir.join("manifest.json");
    assert!(bundled_manifest.exists(), "missing {bundled_manifest:?}");

    let lib_filename = lib_path.file_name().unwrap().to_string_lossy().to_string();
    assert!(
        bundle_dir.join(&lib_filename).exists(),
        "missing bundled dylib {}",
        bundle_dir.join(&lib_filename).display()
    );
    assert!(
        bundle_dir.join("shaders/write_u32.wgsl").exists(),
        "missing bundled shader file"
    );

    let rs = examples.join(format!("{out_name}.rs"));
    assert!(rs.exists(), "missing generated example {rs:?}");

    // Cleanup test artifacts written into the repo.
    let _ = std::fs::remove_file(rs);
    let _ = std::fs::remove_dir_all(&bundle_dir);
}
