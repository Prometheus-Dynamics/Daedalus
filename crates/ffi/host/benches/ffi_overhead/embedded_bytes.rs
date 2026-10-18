//! Cross-process embedded-bytes bench: a Python worker decodes a `bytes` argument.

use super::*;

/// A Python worker that decodes an embedded `bytes` argument (a JSON number array or a base64
/// string) and answers with its length, so the bench measures the real cross-process codec.
fn write_python_embedded_bytes_worker(dir: &Path) -> PathBuf {
    let worker = dir.join("embedded_bytes_worker.py");
    std::fs::write(
        &worker,
        r#"
import base64
import json
import sys

def send(payload, correlation_id=None):
    sys.stdout.write(json.dumps({"protocol_version": 1, "correlation_id": correlation_id, "payload": payload}) + "\n")
    sys.stdout.flush()

send({"type": "hello", "payload": {"protocol_version": 1, "min_protocol_version": 1, "worker_id": "python-embedded-bytes-worker", "backend": "python", "supported_nodes": ["bytes:len"], "capabilities": ["persistent_worker"]}}, "startup")

for line in sys.stdin:
    message = json.loads(line)
    payload = message["payload"]
    if payload["type"] == "ack":
        continue
    request = payload["payload"]
    data = request["args"]["payload"]["value"]["data"]
    size = len(base64.b64decode(data)) if isinstance(data, str) else len(data)
    correlation_id = request.get("correlation_id")
    send({"type": "response", "payload": {"protocol_version": 1, "correlation_id": correlation_id, "outputs": {"out": {"kind": "int", "value": size}}, "events": []}}, correlation_id)
"#,
    )
    .expect("write python embedded bytes worker");
    worker
}

pub(super) fn bench_cross_process_embedded_bytes(c: &mut Criterion) {
    let mut group = c.benchmark_group("ffi_cross_process_embedded_bytes");
    if let Some(python) = executable_available("PYTHON", "python3", "--version") {
        for size in [1024 * 1024, 10 * 1024 * 1024] {
            let root = tempfile::tempdir().expect("embedded bytes worker root");
            let worker = write_python_embedded_bytes_worker(root.path());
            let backend = BackendConfig {
                args: vec![worker.display().to_string()],
                working_dir: Some(root.path().display().to_string()),
                ..BackendConfig::persistent_worker(BackendKind::Python, python.clone(), "bytes_len")
                    .with_entry_module(worker.display().to_string())
            };
            let runner = PersistentWorkerRunner::from_backend(&backend).expect("python runner");
            runner.start().expect("start python embedded bytes worker");
            let request = InvokeRequest {
                protocol_version: daedalus_ffi_core::WORKER_PROTOCOL_VERSION,
                node_id: "bytes:len".into(),
                correlation_id: Some("req-1".into()),
                args: BTreeMap::from([(
                    "payload".into(),
                    WireValue::Bytes(BytePayload {
                        data: vec![7; size],
                        encoding: ByteEncoding::Base64,
                    }),
                )]),
                state: None,
                context: BTreeMap::new(),
            };
            group.throughput(Throughput::Bytes(size as u64));
            group.bench_with_input(BenchmarkId::new("python", size), &request, |b, request| {
                b.iter(|| {
                    black_box(runner.invoke(request.clone()).expect("python bytes invoke"));
                });
            });
            runner
                .shutdown()
                .expect("shutdown python embedded bytes worker");
        }
    }
    group.finish();
}
