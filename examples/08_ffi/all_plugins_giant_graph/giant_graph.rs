mod example_smoke;
mod giant_graph_coverage;

use std::io::Write;

use example_smoke::run_example_giant_graph_smoke_test;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let report = run_example_giant_graph_smoke_test()?;
    let output = serde_json::json!({
        "packages_loaded": report.packages_loaded,
        "nodes_invoked": report.nodes_invoked,
        "edges_validated": report.edges_validated,
        "package_artifacts_checked": report.package_artifacts_checked,
        "expected_errors_checked": report.expected_errors_checked,
        "coverage": report.coverage,
        "telemetry": report.telemetry,
    });
    writeln!(
        std::io::stdout().lock(),
        "{}",
        serde_json::to_string_pretty(&output)?
    )?;
    Ok(())
}
