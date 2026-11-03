use super::*;

#[test]
fn persistent_worker_rejects_unsupported_runner_limits() {
    let config = BackendConfig::persistent_worker(BackendKind::Python, "/bin/sh", "run")
        .with_entry_module("limits");

    let queue_depth = PersistentWorkerRunner::from_backend_with_limits(
        &config,
        &RunnerLimits {
            queue_depth: 2,
            ..RunnerLimits::default()
        },
    );
    assert!(matches!(
        queue_depth,
        Err(RunnerPoolError::UnsupportedRunnerLimit {
            limit: "queue_depth",
            ..
        })
    ));

    let restart_policy = PersistentWorkerRunner::from_backend_with_limits(
        &config,
        &RunnerLimits {
            restart_policy: RunnerRestartPolicy::OnFailure,
            ..RunnerLimits::default()
        },
    );
    assert!(matches!(
        restart_policy,
        Err(RunnerPoolError::UnsupportedRunnerLimit {
            limit: "restart_policy",
            ..
        })
    ));
}
