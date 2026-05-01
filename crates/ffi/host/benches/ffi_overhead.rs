use std::collections::BTreeMap;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use daedalus_ffi_core::{
    BackendConfig, BackendKind, ByteEncoding, BytePayload, FixtureLanguage,
    GeneratedLanguageFixture, GeneratedPackageFixture, InvokeRequest, InvokeResponse,
    PluginPackage, WireValue, generate_scalar_add_fixtures, generate_scalar_add_package_fixtures,
};
#[cfg(feature = "image-payload")]
use daedalus_ffi_core::{ImageLayout, ImagePayload, ScalarDType};
use daedalus_ffi_host::{
    BackendRunner, BackendRunnerFactory, FfiHostTelemetry, HostInstallPlan, PayloadLeaseScope,
    PayloadLeaseTable, PersistentWorkerRunner, RunnerPool, RunnerPoolError, install_plan_runners,
};
use daedalus_transport::{AccessMode, Payload};

#[derive(Clone)]
struct EchoRunner {
    response: InvokeResponse,
    state: Arc<Mutex<Option<WireValue>>>,
}

impl EchoRunner {
    fn new(response: InvokeResponse) -> Self {
        Self {
            response,
            state: Arc::new(Mutex::new(None)),
        }
    }
}

impl BackendRunner for EchoRunner {
    fn invoke(&self, request: InvokeRequest) -> Result<InvokeResponse, RunnerPoolError> {
        let mut response = self.response.clone();
        response.correlation_id = request.correlation_id;
        if let Some(value) = request.args.get("payload") {
            response.outputs.insert("out".into(), value.clone());
        }
        if let Some(value) = request.state {
            *self
                .state
                .lock()
                .map_err(|_| RunnerPoolError::LockPoisoned)? = Some(value.clone());
            response.state = Some(value);
        }
        Ok(response)
    }
}

struct FixtureRunnerFactory {
    response: InvokeResponse,
}

impl BackendRunnerFactory for FixtureRunnerFactory {
    fn build_runner(
        &self,
        _node_id: &str,
        _backend: &BackendConfig,
    ) -> Result<Arc<dyn BackendRunner>, RunnerPoolError> {
        Ok(Arc::new(EchoRunner::new(self.response.clone())))
    }
}

struct PayloadHandleRunner {
    response: InvokeResponse,
    leases: PayloadLeaseTable,
}

impl BackendRunner for PayloadHandleRunner {
    fn invoke(&self, request: InvokeRequest) -> Result<InvokeResponse, RunnerPoolError> {
        if let Some(WireValue::Handle(handle)) = request.args.get("payload") {
            black_box(self.leases.resolve(handle)?);
        }
        let mut response = self.response.clone();
        response.correlation_id = request.correlation_id;
        if let Some(value) = request.args.get("payload") {
            response.outputs.insert("out".into(), value.clone());
        }
        Ok(response)
    }
}

struct PayloadHandleRunnerFactory {
    response: InvokeResponse,
    leases: PayloadLeaseTable,
}

impl BackendRunnerFactory for PayloadHandleRunnerFactory {
    fn build_runner(
        &self,
        _node_id: &str,
        _backend: &BackendConfig,
    ) -> Result<Arc<dyn BackendRunner>, RunnerPoolError> {
        Ok(Arc::new(PayloadHandleRunner {
            response: self.response.clone(),
            leases: self.leases.clone(),
        }))
    }
}

fn worker_fixtures() -> Vec<GeneratedLanguageFixture> {
    generate_scalar_add_fixtures()
        .expect("fixtures")
        .into_iter()
        .filter(|fixture| {
            fixture.schema.nodes[0].backend != BackendKind::Rust
                && fixture.schema.nodes[0].backend != BackendKind::CCpp
        })
        .collect()
}

fn build_pool(fixture: &GeneratedLanguageFixture) -> (RunnerPool, BackendConfig) {
    build_pool_with_options(fixture, None)
}

fn build_pool_with_telemetry(
    fixture: &GeneratedLanguageFixture,
) -> (RunnerPool, BackendConfig, FfiHostTelemetry) {
    let telemetry = FfiHostTelemetry::new();
    let (pool, backend) = build_pool_with_options(fixture, Some(telemetry.clone()));
    (pool, backend, telemetry)
}

fn build_pool_with_options(
    fixture: &GeneratedLanguageFixture,
    telemetry: Option<FfiHostTelemetry>,
) -> (RunnerPool, BackendConfig) {
    let plan = HostInstallPlan::from_schema_and_backends(&fixture.schema, &fixture.backends)
        .expect("install plan");
    let factory = FixtureRunnerFactory {
        response: fixture.expected_response.clone(),
    };
    let mut pool = match telemetry {
        Some(telemetry) => RunnerPool::new().with_ffi_telemetry(telemetry),
        None => RunnerPool::new(),
    };
    install_plan_runners(&mut pool, &plan, &factory).expect("install runner");
    let node = &fixture.schema.nodes[0];
    let backend = fixture.backends.get(&node.id).expect("backend").clone();
    (pool, backend)
}

fn bytes_fixture(size: usize) -> GeneratedLanguageFixture {
    let mut fixture = worker_fixtures()
        .into_iter()
        .find(|fixture| fixture.language == FixtureLanguage::Python)
        .expect("python fixture");
    fixture.request.args = BTreeMap::from([(
        "payload".into(),
        WireValue::Bytes(BytePayload {
            data: vec![7; size],
            encoding: ByteEncoding::Raw,
        }),
    )]);
    fixture.expected_response.outputs = BTreeMap::from([("out".into(), WireValue::Int(0))]);
    fixture
}

fn payload_handle_fixture(size: usize) -> (GeneratedLanguageFixture, PayloadLeaseTable) {
    let mut fixture = worker_fixtures()
        .into_iter()
        .find(|fixture| fixture.language == FixtureLanguage::Python)
        .expect("python fixture");
    let leases = PayloadLeaseTable::default();
    let payload = Payload::bytes_with_type_key("bytes", Arc::<[u8]>::from(vec![7; size]));
    let handle = leases
        .insert(
            format!("payload-{size}"),
            payload,
            AccessMode::Read,
            PayloadLeaseScope::Runner,
        )
        .expect("lease payload");
    fixture.request.args = BTreeMap::from([("payload".into(), handle)]);
    fixture.expected_response.outputs = BTreeMap::from([("out".into(), WireValue::Int(0))]);
    (fixture, leases)
}

fn payload_handle_request(size: usize) -> InvokeRequest {
    let leases = PayloadLeaseTable::default();
    let payload = Payload::bytes_with_type_key("bytes", Arc::<[u8]>::from(vec![7; size]));
    let handle = leases
        .insert(
            format!("payload-{size}"),
            payload,
            AccessMode::Read,
            PayloadLeaseScope::Runner,
        )
        .expect("lease payload");
    InvokeRequest {
        protocol_version: daedalus_ffi_core::WORKER_PROTOCOL_VERSION,
        node_id: "payload:len".into(),
        correlation_id: Some("req-1".into()),
        args: BTreeMap::from([("payload".into(), handle)]),
        state: None,
        context: BTreeMap::new(),
    }
}

fn executable_available(env_var: &str, default: &str, version_arg: &str) -> Option<String> {
    let executable = std::env::var(env_var).unwrap_or_else(|_| default.to_string());
    std::process::Command::new(&executable)
        .arg(version_arg)
        .output()
        .ok()
        .map(|_| executable)
}

fn java_tools_available() -> Option<(String, String)> {
    let javac = std::env::var("JAVAC").unwrap_or_else(|_| "javac".to_string());
    let java = std::env::var("JAVA").unwrap_or_else(|_| "java".to_string());
    let javac_ok = std::process::Command::new(&javac)
        .arg("--version")
        .output()
        .is_ok();
    let java_ok = std::process::Command::new(&java)
        .arg("-version")
        .output()
        .is_ok();
    if javac_ok && java_ok {
        Some((javac, java))
    } else {
        None
    }
}

fn write_python_payload_worker(dir: &Path) -> PathBuf {
    let worker = dir.join("payload_worker.py");
    std::fs::write(
        &worker,
        r#"
import json
import sys

def send(payload, correlation_id=None):
    sys.stdout.write(json.dumps({"protocol_version": 1, "correlation_id": correlation_id, "payload": payload}) + "\n")
    sys.stdout.flush()

send({"type": "hello", "payload": {"protocol_version": 1, "min_protocol_version": 1, "worker_id": "python-payload-worker", "backend": "python", "supported_nodes": ["payload:len"], "capabilities": ["persistent_worker", "payload_handle"]}}, "startup")

for line in sys.stdin:
    message = json.loads(line)
    payload = message["payload"]
    if payload["type"] == "ack":
        continue
    request = payload["payload"]
    handle = request["args"]["payload"]["value"]
    size = handle["metadata"]["bytes_estimate"]
    correlation_id = request.get("correlation_id")
    send({"type": "response", "payload": {"protocol_version": 1, "correlation_id": correlation_id, "outputs": {"out": {"kind": "int", "value": size}}, "events": []}}, correlation_id)
"#,
    )
    .expect("write python payload worker");
    worker
}

fn write_node_payload_worker(dir: &Path) -> PathBuf {
    let worker = dir.join("payload_worker.mjs");
    std::fs::write(
        &worker,
        r#"
import readline from 'node:readline';

function send(payload, correlationId = null) {
  process.stdout.write(JSON.stringify({ protocol_version: 1, correlation_id: correlationId, payload }) + '\n');
}

send({ type: 'hello', payload: { protocol_version: 1, min_protocol_version: 1, worker_id: 'node-payload-worker', backend: 'node', supported_nodes: ['payload:len'], capabilities: ['persistent_worker', 'payload_handle'] } }, 'startup');

const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
for await (const line of rl) {
  const message = JSON.parse(line);
  const payload = message.payload;
  if (payload.type === 'ack') {
    continue;
  }
  const request = payload.payload;
  const handle = request.args.payload.value;
  const size = handle.metadata.bytes_estimate;
  const correlationId = request.correlation_id ?? null;
  send({ type: 'response', payload: { protocol_version: 1, correlation_id: correlationId, outputs: { out: { kind: 'int', value: size } }, events: [] } }, correlationId);
}
"#,
    )
    .expect("write node payload worker");
    worker
}

fn write_java_payload_worker(dir: &Path, javac: &str) -> PathBuf {
    let classes = dir.join("classes");
    std::fs::create_dir_all(&classes).expect("create java classes");
    let worker = dir.join("PayloadWorker.java");
    std::fs::write(
        &worker,
        r#"
import java.io.BufferedReader;
import java.io.InputStreamReader;

public final class PayloadWorker {
    private static void send(String payload, String correlationId) {
        System.out.println("{\"protocol_version\":1,\"correlation_id\":\"" + correlationId + "\",\"payload\":" + payload + "}");
        System.out.flush();
    }

    private static long sizeFrom(String line) {
        String marker = "\"bytes_estimate\":";
        int start = line.indexOf(marker);
        if (start < 0) {
            return 0;
        }
        start += marker.length();
        int end = start;
        while (end < line.length() && Character.isDigit(line.charAt(end))) {
            end += 1;
        }
        return Long.parseLong(line.substring(start, end));
    }

    public static void main(String[] args) throws Exception {
        send("{\"type\":\"hello\",\"payload\":{\"protocol_version\":1,\"min_protocol_version\":1,\"worker_id\":\"java-payload-worker\",\"backend\":\"java\",\"supported_nodes\":[\"payload:len\"],\"capabilities\":[\"persistent_worker\",\"payload_handle\"]}}", "startup");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.contains("\"type\":\"ack\"")) {
                continue;
            }
            long size = sizeFrom(line);
            send("{\"type\":\"response\",\"payload\":{\"protocol_version\":1,\"correlation_id\":\"req-1\",\"outputs\":{\"out\":{\"kind\":\"int\",\"value\":" + size + "}},\"events\":[]}}", "req-1");
        }
    }
}
"#,
    )
    .expect("write java payload worker");
    let status = std::process::Command::new(javac)
        .arg("-d")
        .arg(&classes)
        .arg(&worker)
        .status()
        .expect("spawn javac");
    assert!(status.success(), "javac failed for payload worker");
    classes
}

fn stateful_fixture() -> GeneratedLanguageFixture {
    let mut fixture = worker_fixtures()
        .into_iter()
        .find(|fixture| fixture.language == FixtureLanguage::Node)
        .expect("node fixture");
    fixture.schema.nodes[0].stateful = true;
    fixture.request.state = Some(WireValue::Int(1));
    fixture.expected_response.state = Some(WireValue::Int(1));
    fixture
}

fn bench_cold_load(c: &mut Criterion) {
    let fixtures = generate_scalar_add_fixtures().expect("fixtures");
    let mut group = c.benchmark_group("ffi_cold_load");
    for fixture in fixtures {
        group.bench_with_input(
            BenchmarkId::new("install_plan_runners", fixture.language.as_str()),
            &fixture,
            |b, fixture| {
                b.iter(|| {
                    let plan = HostInstallPlan::from_schema_and_backends(
                        &fixture.schema,
                        &fixture.backends,
                    )
                    .expect("install plan");
                    let factory = FixtureRunnerFactory {
                        response: fixture.expected_response.clone(),
                    };
                    let mut pool = RunnerPool::new();
                    black_box(install_plan_runners(&mut pool, &plan, &factory).expect("install"));
                });
            },
        );
    }
    group.finish();
}

fn bench_warm_invoke(c: &mut Criterion) {
    let fixtures = generate_scalar_add_fixtures().expect("fixtures");
    let mut group = c.benchmark_group("ffi_warm_invoke");
    for fixture in fixtures {
        if fixture.schema.nodes[0].backend == BackendKind::Rust
            || fixture.schema.nodes[0].backend == BackendKind::CCpp
        {
            group.bench_with_input(
                BenchmarkId::new("direct_in_process", fixture.language.as_str()),
                &fixture.expected_response,
                |b, response| {
                    b.iter(|| {
                        black_box(response.clone());
                    });
                },
            );
        } else {
            let (pool, backend) = build_pool(&fixture);
            group.bench_with_input(
                BenchmarkId::new("runner_pool", fixture.language.as_str()),
                &fixture.request,
                |b, request| {
                    b.iter(|| {
                        black_box(pool.invoke(&backend, request.clone()).expect("invoke"));
                    });
                },
            );
        }
    }
    group.finish();
}

fn bench_worker_warm_invoke(c: &mut Criterion) {
    let fixtures = worker_fixtures();
    let mut group = c.benchmark_group("ffi_worker_warm_invoke");
    for fixture in fixtures {
        let (pool, backend) = build_pool(&fixture);
        group.bench_with_input(
            BenchmarkId::new("runner_pool", fixture.language.as_str()),
            &fixture.request,
            |b, request| {
                b.iter(|| {
                    black_box(pool.invoke(&backend, request.clone()).expect("invoke"));
                });
            },
        );
    }
    group.finish();
}

fn bench_stateful_warm_invoke(c: &mut Criterion) {
    let fixture = stateful_fixture();
    let (pool, backend) = build_pool(&fixture);
    c.bench_function("ffi_stateful_warm_invoke/node", |b| {
        b.iter(|| {
            black_box(
                pool.invoke(&backend, fixture.request.clone())
                    .expect("stateful invoke"),
            );
        });
    });
}

fn bench_embedded_bytes_payloads(c: &mut Criterion) {
    let mut group = c.benchmark_group("ffi_embedded_bytes_payload");
    for size in [1024 * 1024, 10 * 1024 * 1024] {
        let fixture = bytes_fixture(size);
        let (pool, backend) = build_pool(&fixture);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            &fixture.request,
            |b, request| {
                b.iter(|| {
                    black_box(
                        pool.invoke(&backend, request.clone())
                            .expect("bytes invoke"),
                    );
                });
            },
        );
    }
    group.finish();
}

fn bench_payload_handle_refs(c: &mut Criterion) {
    let mut group = c.benchmark_group("ffi_payload_handle_ref");
    for size in [1024 * 1024, 10 * 1024 * 1024] {
        let (fixture, leases) = payload_handle_fixture(size);
        let plan = HostInstallPlan::from_schema_and_backends(&fixture.schema, &fixture.backends)
            .expect("install plan");
        let factory = PayloadHandleRunnerFactory {
            response: fixture.expected_response.clone(),
            leases,
        };
        let mut pool = RunnerPool::new();
        install_plan_runners(&mut pool, &plan, &factory).expect("install runner");
        let node = &fixture.schema.nodes[0];
        let backend = fixture.backends.get(&node.id).expect("backend").clone();
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            &fixture.request,
            |b, request| {
                b.iter(|| {
                    black_box(
                        pool.invoke(&backend, request.clone())
                            .expect("payload handle invoke"),
                    );
                });
            },
        );
    }
    group.finish();
}

fn bench_cross_process_payload_handle_refs(c: &mut Criterion) {
    let mut group = c.benchmark_group("ffi_cross_process_payload_handle_ref");
    for size in [1024 * 1024, 10 * 1024 * 1024] {
        group.throughput(Throughput::Bytes(size as u64));
        let request = payload_handle_request(size);

        if let Some(python) = executable_available("PYTHON", "python", "--version") {
            let root = tempfile::tempdir().expect("python payload worker root");
            let worker = write_python_payload_worker(root.path());
            let backend = BackendConfig {
                backend: BackendKind::Python,
                runtime_model: daedalus_ffi_core::BackendRuntimeModel::PersistentWorker,
                entry_module: Some(worker.display().to_string()),
                entry_class: None,
                entry_symbol: Some("payload_len".into()),
                executable: Some(python),
                args: vec![worker.display().to_string()],
                classpath: Vec::new(),
                native_library_paths: Vec::new(),
                working_dir: Some(root.path().display().to_string()),
                env: BTreeMap::new(),
                options: BTreeMap::new(),
            };
            let runner = PersistentWorkerRunner::from_backend(&backend).expect("python runner");
            runner.start().expect("start python payload worker");
            group.bench_with_input(BenchmarkId::new("python", size), &request, |b, request| {
                b.iter(|| {
                    black_box(runner.invoke(request.clone()).expect("python invoke"));
                });
            });
            runner.shutdown().expect("shutdown python payload worker");
        }

        if let Some(node) = executable_available("NODE", "node", "--version") {
            let root = tempfile::tempdir().expect("node payload worker root");
            let worker = write_node_payload_worker(root.path());
            let backend = BackendConfig {
                backend: BackendKind::Node,
                runtime_model: daedalus_ffi_core::BackendRuntimeModel::PersistentWorker,
                entry_module: Some(worker.display().to_string()),
                entry_class: None,
                entry_symbol: Some("payload_len".into()),
                executable: Some(node),
                args: vec![worker.display().to_string()],
                classpath: Vec::new(),
                native_library_paths: Vec::new(),
                working_dir: Some(root.path().display().to_string()),
                env: BTreeMap::new(),
                options: BTreeMap::new(),
            };
            let runner = PersistentWorkerRunner::from_backend(&backend).expect("node runner");
            runner.start().expect("start node payload worker");
            group.bench_with_input(BenchmarkId::new("node", size), &request, |b, request| {
                b.iter(|| {
                    black_box(runner.invoke(request.clone()).expect("node invoke"));
                });
            });
            runner.shutdown().expect("shutdown node payload worker");
        }

        if let Some((javac, java)) = java_tools_available() {
            let root = tempfile::tempdir().expect("java payload worker root");
            let classes = write_java_payload_worker(root.path(), &javac);
            let backend = BackendConfig {
                backend: BackendKind::Java,
                runtime_model: daedalus_ffi_core::BackendRuntimeModel::PersistentWorker,
                entry_module: None,
                entry_class: Some("PayloadWorker".into()),
                entry_symbol: Some("payload_len".into()),
                executable: Some(java),
                args: vec![
                    "-cp".into(),
                    classes.display().to_string(),
                    "PayloadWorker".into(),
                ],
                classpath: vec![classes.display().to_string()],
                native_library_paths: Vec::new(),
                working_dir: Some(root.path().display().to_string()),
                env: BTreeMap::new(),
                options: BTreeMap::new(),
            };
            let runner = PersistentWorkerRunner::from_backend(&backend).expect("java runner");
            runner.start().expect("start java payload worker");
            group.bench_with_input(BenchmarkId::new("java", size), &request, |b, request| {
                b.iter(|| {
                    black_box(runner.invoke(request.clone()).expect("java invoke"));
                });
            });
            runner.shutdown().expect("shutdown java payload worker");
        }
    }
    group.finish();
}

#[cfg(feature = "image-payload")]
fn bench_image_payload(c: &mut Criterion) {
    let mut group = c.benchmark_group("ffi_image_payload");
    {
        let (name, width, height, channels) = ("rgba_1080p", 1920_u32, 1080_u32, 4_u8);
        let size = width as usize * height as usize * channels as usize;
        let mut fixture = worker_fixtures()
            .into_iter()
            .find(|fixture| fixture.language == FixtureLanguage::Python)
            .expect("python fixture");
        fixture.request.args = BTreeMap::from([(
            "payload".into(),
            WireValue::Image(ImagePayload {
                data: vec![127; size],
                width,
                height,
                channels,
                dtype: ScalarDType::U8,
                layout: ImageLayout::Hwc,
            }),
        )]);
        fixture.expected_response.outputs = BTreeMap::from([("out".into(), WireValue::Int(0))]);
        let (pool, backend) = build_pool(&fixture);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(name),
            &fixture.request,
            |b, request| {
                b.iter(|| {
                    black_box(
                        pool.invoke(&backend, request.clone())
                            .expect("image invoke"),
                    );
                });
            },
        );
    }
    group.finish();
}

#[cfg(not(feature = "image-payload"))]
fn bench_image_payload(_c: &mut Criterion) {}

fn bench_runner_reuse(c: &mut Criterion) {
    let fixture = worker_fixtures()
        .into_iter()
        .next()
        .expect("worker fixture");
    let (pool, backend) = build_pool(&fixture);
    c.bench_function("ffi_runner_reuse/get_existing", |b| {
        b.iter(|| {
            black_box(pool.get(&backend).expect("runner"));
            black_box(pool.telemetry().reuses);
        });
    });
}

fn bench_telemetry_overhead(c: &mut Criterion) {
    let fixture = worker_fixtures()
        .into_iter()
        .next()
        .expect("worker fixture");
    let (plain_pool, plain_backend) = build_pool(&fixture);
    let (telemetry_pool, telemetry_backend, telemetry) = build_pool_with_telemetry(&fixture);
    let mut group = c.benchmark_group("ffi_telemetry_overhead");
    group.bench_function("warm_invoke/plain", |b| {
        b.iter(|| {
            black_box(
                plain_pool
                    .invoke(&plain_backend, fixture.request.clone())
                    .expect("plain invoke"),
            );
        });
    });
    group.bench_function("warm_invoke/telemetry", |b| {
        b.iter(|| {
            black_box(
                telemetry_pool
                    .invoke(&telemetry_backend, fixture.request.clone())
                    .expect("telemetry invoke"),
            );
            black_box(telemetry.snapshot());
        });
    });

    let (payload_fixture, _) = payload_handle_fixture(1024 * 1024);
    let (payload_pool, payload_backend) = build_pool(&payload_fixture);
    let (telemetry_payload_pool, telemetry_payload_backend, payload_telemetry) =
        build_pool_with_telemetry(&payload_fixture);
    group.throughput(Throughput::Bytes(1024 * 1024));
    group.bench_function("payload_handle_1mib/plain", |b| {
        b.iter(|| {
            black_box(
                payload_pool
                    .invoke(&payload_backend, payload_fixture.request.clone())
                    .expect("plain payload invoke"),
            );
        });
    });
    group.bench_function("payload_handle_1mib/telemetry", |b| {
        b.iter(|| {
            black_box(
                telemetry_payload_pool
                    .invoke(&telemetry_payload_backend, payload_fixture.request.clone())
                    .expect("telemetry payload invoke"),
            );
            black_box(payload_telemetry.snapshot());
        });
    });
    group.finish();
}

fn bench_batch_invoke(c: &mut Criterion) {
    let fixture = worker_fixtures()
        .into_iter()
        .next()
        .expect("worker fixture");
    let (pool, backend) = build_pool(&fixture);
    let requests = vec![fixture.request.clone(); 64];
    c.bench_function("ffi_batch_invoke/64", |b| {
        b.iter(|| {
            black_box(
                pool.invoke_batch(&backend, requests.clone())
                    .expect("batch invoke"),
            );
        });
    });
}

fn write_package_fixture(fixture: &GeneratedPackageFixture) -> tempfile::TempDir {
    let root = tempfile::tempdir().expect("package root");
    for file in &fixture.files {
        let path = root.path().join(&file.path);
        std::fs::create_dir_all(path.parent().expect("artifact parent")).expect("mkdir");
        std::fs::write(path, &file.contents).expect("write artifact");
    }
    root
}

fn bench_package_load(c: &mut Criterion) {
    let mut fixtures = generate_scalar_add_package_fixtures().expect("package fixtures");
    let mut fixture = fixtures
        .drain(..)
        .find(|fixture| fixture.language == FixtureLanguage::Java)
        .expect("java package fixture");
    let root = write_package_fixture(&fixture);
    fixture
        .package
        .stamp_integrity(root.path())
        .expect("stamp package");
    let descriptor = root.path().join("plugin.json");
    fixture
        .package
        .write_descriptor(&descriptor)
        .expect("write descriptor");

    c.bench_function("ffi_package_load/java_descriptor_verify", |b| {
        b.iter(|| {
            black_box(
                PluginPackage::read_descriptor_and_verify(&descriptor, root.path())
                    .expect("load package"),
            );
        });
    });
}

criterion_group!(
    benches,
    bench_cold_load,
    bench_warm_invoke,
    bench_worker_warm_invoke,
    bench_stateful_warm_invoke,
    bench_embedded_bytes_payloads,
    bench_payload_handle_refs,
    bench_cross_process_payload_handle_refs,
    bench_image_payload,
    bench_runner_reuse,
    bench_telemetry_overhead,
    bench_batch_invoke,
    bench_package_load
);
criterion_main!(benches);
