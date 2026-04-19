mod support;

use support::docker_examples::{DockerExamples, output_text};

#[test]
#[ignore = "requires docker"]
fn docker_cpu_branch_reports_expected_result() {
    let examples = DockerExamples::start();
    let output = examples.run_output("/workspace/target/debug/examples/cpu_branch");
    assert!(
        output.status.success(),
        "cpu_branch failed\n{}",
        output_text(&output)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("branch result: 25"));
    assert!(stdout.contains("telemetry:"));
}

#[test]
#[ignore = "requires docker"]
fn docker_cpu_text_writes_expected_output() {
    let examples = DockerExamples::start();
    let output = examples.run_output(
        "/workspace/target/debug/examples/cpu_text && cat /workspace/crates/daedalus/examples/assets/output_text.txt",
    );
    assert!(
        output.status.success(),
        "cpu_text failed\n{}",
        output_text(&output)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("wrote examples/assets/output_text.txt"));
    assert!(stdout.contains("HELLO, WORLD, DAEDALUS"));
}

#[test]
#[ignore = "requires docker"]
fn docker_cpu_image_writes_output_image() {
    let examples = DockerExamples::start();
    let output = examples.run_output(
        "/workspace/target/debug/examples/cpu_image && test -f /workspace/crates/daedalus/examples/assets/output_cpu.png && echo output_cpu.png:ok",
    );
    assert!(
        output.status.success(),
        "cpu_image failed\n{}",
        output_text(&output)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("registry nodes installed:"));
    assert!(stdout.contains("wrote examples/assets/output_cpu.png"));
    assert!(stdout.contains("output_cpu.png:ok"));
}
