use std::process::Command;

fn has_cmd(cmd: &str, arg: &str) -> bool {
    Command::new(cmd).arg(arg).output().is_ok()
}

fn run_harness(lang: &str) {
    if std::env::var_os("DAEDALUS_TEST_GPU").is_none() {
        eprintln!("skipping: set DAEDALUS_TEST_GPU=1 to run GPU shader tests");
        return;
    }

    // Ensure the language runtime exists before we spend time building.
    match lang {
        "python" => {
            let py = std::env::var("PYTHON").unwrap_or_else(|_| "python".to_string());
            if !has_cmd(&py, "--version") {
                eprintln!("skipping: python interpreter not found");
                return;
            }
        }
        "java" => {
            let javac = std::env::var("JAVAC").unwrap_or_else(|_| "javac".to_string());
            let java = std::env::var("JAVA").unwrap_or_else(|_| "java".to_string());
            if !has_cmd(&javac, "--version") || !has_cmd(&java, "-version") {
                eprintln!("skipping: java/javac not found");
                return;
            }
        }
        "node" => {
            let node = std::env::var("NODE").unwrap_or_else(|_| "node".to_string());
            if !has_cmd(&node, "--version") {
                eprintln!("skipping: node interpreter not found");
                return;
            }
        }
        other => panic!("unknown lang {other}"),
    }

    let strict = std::env::var_os("DAEDALUS_TEST_GPU_STRICT").is_some();
    let status = Command::new("cargo")
        .args([
            "run",
            "-p",
            "daedalus-ffi",
            "--features",
            "gpu-wgpu",
            "--example",
            "manifest_shader_harness",
            "--",
            lang,
        ])
        .status()
        .expect("run manifest_shader_harness");

    if !status.success() {
        let msg = format!("gpu shader harness failed (status={status})");
        if strict {
            panic!("{msg}");
        } else {
            eprintln!("skipping: {msg} (set DAEDALUS_TEST_GPU_STRICT=1 to fail)");
        }
    }
}

#[test]
fn python_manifest_shader_suite() {
    run_harness("python");
}

#[test]
fn node_manifest_shader_suite() {
    run_harness("node");
}

#[test]
fn java_manifest_shader_suite() {
    run_harness("java");
}
