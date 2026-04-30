use std::collections::BTreeMap;
use std::hint::black_box;
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
    BackendRunner, BackendRunnerFactory, HostInstallPlan, RunnerPool, RunnerPoolError,
    install_plan_runners,
};

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
    let plan = HostInstallPlan::from_schema_and_backends(&fixture.schema, &fixture.backends)
        .expect("install plan");
    let factory = FixtureRunnerFactory {
        response: fixture.expected_response.clone(),
    };
    let mut pool = RunnerPool::new();
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

fn bench_bytes_payloads(c: &mut Criterion) {
    let mut group = c.benchmark_group("ffi_bytes_payload");
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

#[cfg(feature = "image-payload")]
fn bench_image_payload(c: &mut Criterion) {
    let mut group = c.benchmark_group("ffi_image_payload");
    for (name, width, height, channels) in [("rgba_1080p", 1920_u32, 1080_u32, 4_u8)] {
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
    bench_bytes_payloads,
    bench_image_payload,
    bench_runner_reuse,
    bench_batch_invoke,
    bench_package_load
);
criterion_main!(benches);
