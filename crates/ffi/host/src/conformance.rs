use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use daedalus_ffi_core::{
    BackendRuntimeModel, FixtureLanguage, GeneratedLanguageFixture, InvokeRequest, InvokeResponse,
    WireValue, generate_canonical_fixtures, generate_scalar_add_fixtures,
};
use thiserror::Error;

use crate::{
    BackendRunner, BackendRunnerFactory, HostInstallError, HostInstallPlan, RunnerHealth,
    RunnerPool, RunnerPoolError, decode_response, install_plan_runners,
};

#[derive(Clone, Debug, PartialEq)]
pub struct FixtureHarnessReport {
    pub invoked_languages: Vec<FixtureLanguage>,
    pub normalized_outputs: BTreeMap<String, WireValue>,
    pub runner_start_count: u64,
    pub runner_reuse_count: u64,
}

#[derive(Debug, Error)]
pub enum FixtureHarnessError {
    #[error("fixture generation failed: {0}")]
    Fixture(#[from] daedalus_ffi_core::FfiContractError),
    #[error("fixture install failed for {language:?}: {source}")]
    Install {
        language: FixtureLanguage,
        source: HostInstallError,
    },
    #[error("fixture runner failed for {language:?}: {source}")]
    Runner {
        language: FixtureLanguage,
        source: RunnerPoolError,
    },
    #[error("fixture response decode failed for {language:?}: {source}")]
    Decode {
        language: FixtureLanguage,
        source: crate::ResponseDecodeError,
    },
    #[error("fixture output mismatch for {language:?}: expected {expected:?}, found {found:?}")]
    OutputMismatch {
        language: FixtureLanguage,
        expected: BTreeMap<String, WireValue>,
        found: BTreeMap<String, WireValue>,
    },
}

pub fn run_scalar_add_generated_fixture_harness()
-> Result<FixtureHarnessReport, FixtureHarnessError> {
    run_generated_fixture_harness(generate_scalar_add_fixtures()?)
}

pub fn run_canonical_generated_fixture_harness() -> Result<FixtureHarnessReport, FixtureHarnessError>
{
    run_generated_fixture_harness(generate_canonical_fixtures()?)
}

pub fn run_generated_fixture_harness(
    fixtures: Vec<GeneratedLanguageFixture>,
) -> Result<FixtureHarnessReport, FixtureHarnessError> {
    let mut invoked_languages = Vec::with_capacity(fixtures.len());
    let mut first_outputs: Option<BTreeMap<String, WireValue>> = None;
    let mut baseline_outputs_by_node: BTreeMap<String, BTreeMap<String, WireValue>> =
        BTreeMap::new();
    let mut start_count = 0;
    let mut reuse_count = 0;

    for fixture in fixtures {
        let plan = HostInstallPlan::from_schema_and_backends(&fixture.schema, &fixture.backends)
            .map_err(|source| FixtureHarnessError::Install {
                language: fixture.language,
                source,
            })?;
        let factory = FixtureRunnerFactory::new(fixture.expected_response.clone());
        let mut pool = RunnerPool::new();
        install_plan_runners(&mut pool, &plan, &factory).map_err(|source| {
            FixtureHarnessError::Install {
                language: fixture.language,
                source,
            }
        })?;

        let node = &fixture.schema.nodes[0];
        let backend =
            fixture
                .backends
                .get(&node.id)
                .ok_or_else(|| FixtureHarnessError::Install {
                    language: fixture.language,
                    source: HostInstallError::MissingBackend {
                        node_id: node.id.clone(),
                    },
                })?;
        let mut outputs = BTreeMap::new();
        for _ in 0..2 {
            let response = if backend.runtime_model == BackendRuntimeModel::InProcessAbi {
                let mut response = fixture.expected_response.clone();
                response.correlation_id = fixture.request.correlation_id.clone();
                response
            } else {
                pool.invoke(backend, fixture.request.clone())
                    .map_err(|source| FixtureHarnessError::Runner {
                        language: fixture.language,
                        source,
                    })?
            };
            let decoded = decode_response(response, fixture.request.correlation_id.as_deref())
                .map_err(|source| FixtureHarnessError::Decode {
                    language: fixture.language,
                    source,
                })?;
            outputs = decoded.outputs().clone();
        }
        if outputs != fixture.expected_response.outputs {
            return Err(FixtureHarnessError::OutputMismatch {
                language: fixture.language,
                expected: fixture.expected_response.outputs,
                found: outputs,
            });
        }
        if let Some(baseline) = baseline_outputs_by_node.get(&node.id) {
            if baseline != &outputs {
                return Err(FixtureHarnessError::OutputMismatch {
                    language: fixture.language,
                    expected: baseline.clone(),
                    found: outputs,
                });
            }
        } else {
            if first_outputs.is_none() {
                first_outputs = Some(outputs.clone());
            }
            baseline_outputs_by_node.insert(node.id.clone(), outputs);
        }
        invoked_languages.push(fixture.language);
        let telemetry = pool.telemetry();
        start_count += telemetry.starts;
        reuse_count += telemetry.reuses;
    }

    let invoked = invoked_languages.iter().copied().collect::<BTreeSet<_>>();
    invoked_languages = invoked.into_iter().collect();

    Ok(FixtureHarnessReport {
        invoked_languages,
        normalized_outputs: first_outputs.unwrap_or_default(),
        runner_start_count: start_count,
        runner_reuse_count: reuse_count,
    })
}

#[derive(Clone)]
struct FixtureRunnerFactory {
    response: InvokeResponse,
}

impl FixtureRunnerFactory {
    fn new(response: InvokeResponse) -> Self {
        Self { response }
    }
}

impl BackendRunnerFactory for FixtureRunnerFactory {
    fn build_runner(
        &self,
        node_id: &str,
        _backend: &daedalus_ffi_core::BackendConfig,
    ) -> Result<Arc<dyn BackendRunner>, RunnerPoolError> {
        Ok(Arc::new(FixtureRunner {
            node_id: node_id.into(),
            response: self.response.clone(),
        }))
    }
}

struct FixtureRunner {
    node_id: String,
    response: InvokeResponse,
}

impl BackendRunner for FixtureRunner {
    fn health(&self) -> RunnerHealth {
        RunnerHealth::Ready
    }

    fn supported_nodes(&self) -> Option<Vec<String>> {
        Some(vec![self.node_id.clone()])
    }

    fn invoke(&self, request: InvokeRequest) -> Result<InvokeResponse, RunnerPoolError> {
        let mut response = self.response.clone();
        response.correlation_id = request.correlation_id;
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use daedalus_ffi_core::{BackendKind, WORKER_PROTOCOL_VERSION, WireValue};
    use std::sync::Arc;

    #[test]
    fn runs_generated_scalar_fixtures_through_one_host_harness() {
        let report = run_scalar_add_generated_fixture_harness().expect("harness");

        assert_eq!(
            report.invoked_languages,
            vec![
                FixtureLanguage::Rust,
                FixtureLanguage::Python,
                FixtureLanguage::Node,
                FixtureLanguage::Java,
                FixtureLanguage::CCpp,
            ]
        );
        assert_eq!(
            report.normalized_outputs.get("out"),
            Some(&WireValue::Int(42))
        );
        assert_eq!(report.runner_start_count, 3);
        assert_eq!(report.runner_reuse_count, 6);
    }

    #[test]
    fn runs_canonical_fixtures_through_one_host_harness() {
        let report = run_canonical_generated_fixture_harness().expect("harness");

        assert_eq!(
            report.invoked_languages,
            vec![
                FixtureLanguage::Rust,
                FixtureLanguage::Python,
                FixtureLanguage::Node,
                FixtureLanguage::Java,
                FixtureLanguage::CCpp,
            ]
        );
        assert_eq!(
            report.normalized_outputs.get("out"),
            Some(&WireValue::Int(42))
        );
        assert!(report.runner_start_count >= 3);
        assert!(report.runner_reuse_count >= 6);
    }

    #[test]
    fn harness_reports_normalized_output_mismatches() {
        let mut fixtures = generate_scalar_add_fixtures().expect("fixtures");
        let fixture = fixtures
            .iter_mut()
            .find(|fixture| fixture.schema.nodes[0].backend == BackendKind::Node)
            .expect("node fixture");
        fixture
            .expected_response
            .outputs
            .insert("out".into(), WireValue::Int(41));

        assert!(matches!(
            run_generated_fixture_harness(fixtures),
            Err(FixtureHarnessError::OutputMismatch {
                language: FixtureLanguage::Node,
                ..
            })
        ));
    }

    #[test]
    fn failure_fixtures_cover_worker_crash_and_malformed_response() {
        let fixtures = generate_scalar_add_fixtures()
            .expect("fixtures")
            .into_iter()
            .filter(|fixture| fixture.schema.nodes[0].backend != BackendKind::Rust)
            .filter(|fixture| fixture.schema.nodes[0].backend != BackendKind::CCpp)
            .collect::<Vec<_>>();
        assert_eq!(fixtures.len(), 3);

        for fixture in fixtures {
            let plan =
                HostInstallPlan::from_schema_and_backends(&fixture.schema, &fixture.backends)
                    .expect("plan");
            let node = &fixture.schema.nodes[0];
            let backend = fixture.backends.get(&node.id).expect("backend");

            let mut crashing_pool = RunnerPool::new();
            install_plan_runners(&mut crashing_pool, &plan, &CrashingFactory)
                .expect("install crash");
            assert!(
                matches!(
                    crashing_pool.invoke(backend, fixture.request.clone()),
                    Err(RunnerPoolError::Runner(message)) if message == "worker crashed"
                ),
                "crash should be reported for {:?}",
                fixture.language
            );

            let mut malformed_pool = RunnerPool::new();
            install_plan_runners(&mut malformed_pool, &plan, &MalformedFactory)
                .expect("install bad");
            let response = malformed_pool
                .invoke(backend, fixture.request.clone())
                .expect("malformed response");
            assert!(
                decode_response(response, fixture.request.correlation_id.as_deref()).is_err(),
                "malformed response should fail for {:?}",
                fixture.language
            );
        }
    }

    struct CrashingFactory;

    impl BackendRunnerFactory for CrashingFactory {
        fn build_runner(
            &self,
            _node_id: &str,
            _backend: &daedalus_ffi_core::BackendConfig,
        ) -> Result<Arc<dyn BackendRunner>, RunnerPoolError> {
            Ok(Arc::new(CrashingRunner))
        }
    }

    struct CrashingRunner;

    impl BackendRunner for CrashingRunner {
        fn supported_nodes(&self) -> Option<Vec<String>> {
            Some(vec!["ffi.conformance.scalar_add:add".into()])
        }

        fn invoke(&self, _request: InvokeRequest) -> Result<InvokeResponse, RunnerPoolError> {
            Err(RunnerPoolError::Runner("worker crashed".into()))
        }
    }

    struct MalformedFactory;

    impl BackendRunnerFactory for MalformedFactory {
        fn build_runner(
            &self,
            _node_id: &str,
            _backend: &daedalus_ffi_core::BackendConfig,
        ) -> Result<Arc<dyn BackendRunner>, RunnerPoolError> {
            Ok(Arc::new(MalformedRunner))
        }
    }

    struct MalformedRunner;

    impl BackendRunner for MalformedRunner {
        fn supported_nodes(&self) -> Option<Vec<String>> {
            Some(vec!["ffi.conformance.scalar_add:add".into()])
        }

        fn invoke(&self, request: InvokeRequest) -> Result<InvokeResponse, RunnerPoolError> {
            Ok(InvokeResponse {
                protocol_version: WORKER_PROTOCOL_VERSION + 1,
                correlation_id: request.correlation_id,
                outputs: BTreeMap::new(),
                state: None,
                events: Vec::new(),
            })
        }
    }
}
