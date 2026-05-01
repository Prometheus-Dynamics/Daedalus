mod example_smoke;
mod giant_graph_coverage;

#[test]
fn smoke_test_loads_example_packages_and_invokes_transcript_nodes() {
    let report = example_smoke::run_example_package_smoke_test().expect("example smoke");

    assert_eq!(report.packages_loaded, 5);
    assert_eq!(report.nodes_invoked, 100);
    assert_eq!(report.expected_errors_checked, 5);
    assert_eq!(
        report.languages,
        vec![
            daedalus_ffi_core::FixtureLanguage::Rust,
            daedalus_ffi_core::FixtureLanguage::Python,
            daedalus_ffi_core::FixtureLanguage::Node,
            daedalus_ffi_core::FixtureLanguage::Java,
            daedalus_ffi_core::FixtureLanguage::CCpp,
        ]
    );
    assert_eq!(report.runner_start_count, 60);
    assert_eq!(report.runner_reuse_count, 60);
}

#[test]
fn giant_graph_smoke_loads_all_packages_nodes_and_artifacts() {
    let report = example_smoke::run_example_giant_graph_smoke_test().expect("giant graph smoke");

    assert_eq!(report.packages_loaded, 5);
    assert_eq!(report.nodes_invoked, 100);
    assert_eq!(report.edges_validated, 196);
    assert_eq!(report.package_artifacts_checked, 14);
    assert_eq!(report.expected_errors_checked, 5);
    assert_eq!(report.coverage.plugin_packages, 5);
    assert_eq!(report.coverage.node_count, 100);
    assert_eq!(report.coverage.total_gpu_nodes(), 5);
    assert_eq!(report.coverage.total_payload_mode_nodes(), 25);
    assert_eq!(report.telemetry.packages.len(), 5);
    assert!(
        report
            .telemetry
            .backends
            .values()
            .map(|backend| backend.invokes)
            .sum::<u64>()
            > 0
    );
    assert!(
        report
            .telemetry
            .backends
            .values()
            .any(|backend| backend.abi_call_duration > std::time::Duration::ZERO)
    );
}

#[test]
fn showcase_descriptors_match_rust_baseline_surface_across_languages() {
    example_smoke::validate_showcase_descriptors_against_rust_baseline()
        .expect("showcase descriptor baseline");
}
