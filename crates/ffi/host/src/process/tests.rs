use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use daedalus_ffi_core::{
    BackendConfig, BackendKind, BackendRuntimeModel, InvokeEventLevel, InvokeRequest,
    WORKER_PROTOCOL_VERSION, WirePayloadHandle, WireValue,
};
use daedalus_transport::{AccessMode, TypeKey};

use super::*;

fn request() -> InvokeRequest {
    InvokeRequest {
        protocol_version: WORKER_PROTOCOL_VERSION,
        node_id: "demo:add".into(),
        correlation_id: Some("req-1".into()),
        args: BTreeMap::from([
            ("a".into(), WireValue::Int(2)),
            ("b".into(), WireValue::Int(40)),
        ]),
        state: None,
        context: BTreeMap::new(),
    }
}

fn payload_mode_request(
    backing_path: &Path,
    len: u64,
    node_id: &str,
    access: AccessMode,
) -> InvokeRequest {
    let mut metadata = BTreeMap::new();
    metadata.insert(
        "mmap_path".into(),
        serde_json::json!(backing_path.display().to_string()),
    );
    metadata.insert("mmap_offset".into(), serde_json::json!(0));
    metadata.insert("mmap_len".into(), serde_json::json!(len));
    metadata.insert("bytes_estimate".into(), serde_json::json!(len));
    metadata.insert(
        "shared_memory_name".into(),
        serde_json::json!(backing_path.display().to_string()),
    );
    metadata.insert("shared_memory_offset".into(), serde_json::json!(0));
    metadata.insert("shared_memory_len".into(), serde_json::json!(len));
    if node_id == "payload:cow_append_marker" {
        metadata.insert("ownership_mode".into(), serde_json::json!("cow"));
    }
    InvokeRequest {
        protocol_version: WORKER_PROTOCOL_VERSION,
        node_id: node_id.into(),
        correlation_id: Some("payload-req-1".into()),
        args: BTreeMap::from([(
            "payload".into(),
            WireValue::Handle(WirePayloadHandle {
                id: "payload-1".into(),
                type_key: TypeKey::new("bytes"),
                access,
                residency: None,
                layout: None,
                capabilities: Vec::new(),
                metadata,
            }),
        )]),
        state: None,
        context: BTreeMap::new(),
    }
}

fn payload_request(backing_path: &Path, len: u64) -> InvokeRequest {
    payload_mode_request(backing_path, len, "payload:len", AccessMode::Read)
}

fn temp_dir(prefix: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    let dir =
        std::env::temp_dir().join(format!("daedalus_{prefix}_{nanos}_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn python_available() -> Option<String> {
    let python = std::env::var("PYTHON").unwrap_or_else(|_| "python".to_string());
    std::process::Command::new(&python)
        .arg("--version")
        .output()
        .ok()
        .map(|_| python)
}

fn node_available() -> Option<String> {
    let node = std::env::var("NODE").unwrap_or_else(|_| "node".to_string());
    std::process::Command::new(&node)
        .arg("--version")
        .output()
        .ok()
        .map(|_| node)
}

fn java_available() -> Option<(String, String)> {
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

fn write_python_worker(dir: &Path) -> PathBuf {
    let module_path = dir.join("demo_module.py");
    std::fs::write(
        &module_path,
        r#"
LOAD_COUNT = 0
LOAD_COUNT += 1
STATE = 0

def add(a, b):
    global STATE
    STATE += 1
    return a + b, STATE, LOAD_COUNT
"#,
    )
    .expect("write python module");

    let worker_path = dir.join("worker.py");
    std::fs::write(
            &worker_path,
            r#"
import importlib.util
import json
import sys

module_path = sys.argv[1]
spec = importlib.util.spec_from_file_location("demo_module", module_path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

def send(payload, correlation_id=None):
    sys.stdout.write(json.dumps({
        "protocol_version": 1,
        "correlation_id": correlation_id,
        "payload": payload,
    }) + "\n")
    sys.stdout.flush()

send({
    "type": "hello",
    "payload": {
        "protocol_version": 1,
        "min_protocol_version": 1,
        "worker_id": "python-test-worker",
        "backend": "python",
        "supported_nodes": ["demo:add"],
        "capabilities": ["persistent_worker", "state", "events"],
    },
}, "startup")

for line in sys.stdin:
    message = json.loads(line)
    payload = message["payload"]
    if payload["type"] == "ack":
        continue
    if payload["type"] != "invoke":
        send({"type": "error", "payload": {"code": "bad_message", "message": "expected invoke"}}, message.get("correlation_id"))
        continue
    request = payload["payload"]
    args = request.get("args", {})
    a = args["a"]["value"]
    b = args["b"]["value"]
    total, state, loads = module.add(a, b)
    correlation_id = request.get("correlation_id")
    send({"type": "event", "payload": {"level": "info", "message": "python worker invoked", "metadata": {"state": state}}}, correlation_id)
    send({
        "type": "response",
        "payload": {
            "protocol_version": 1,
            "correlation_id": correlation_id,
            "outputs": {
                "out": {"kind": "int", "value": total},
                "loads": {"kind": "int", "value": loads}
            },
            "state": {"kind": "int", "value": state},
            "events": []
        }
    }, correlation_id)
"#,
        )
        .expect("write python worker");
    worker_path
}

fn write_node_worker(dir: &Path) -> PathBuf {
    let module_path = dir.join("demo_module.mjs");
    std::fs::write(
        &module_path,
        r#"
export const LOAD_COUNT = 1;
let state = 0;

export function add(a, b) {
  state += 1;
  return { total: a + b, state, loads: LOAD_COUNT };
}
"#,
    )
    .expect("write node module");

    let worker_path = dir.join("worker.mjs");
    std::fs::write(
            &worker_path,
            r#"
import readline from 'node:readline';
import { pathToFileURL } from 'node:url';

const modulePath = process.argv[2];
const module = await import(pathToFileURL(modulePath).href);

function send(payload, correlationId = null) {
  process.stdout.write(JSON.stringify({
    protocol_version: 1,
    correlation_id: correlationId,
    payload,
  }) + '\n');
}

send({
  type: 'hello',
  payload: {
    protocol_version: 1,
    min_protocol_version: 1,
    worker_id: 'node-test-worker',
    backend: 'node',
    supported_nodes: ['demo:add'],
    capabilities: ['persistent_worker', 'state', 'events'],
  },
}, 'startup');

const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
for await (const line of rl) {
  const message = JSON.parse(line);
  const payload = message.payload;
  if (payload.type === 'ack') {
    continue;
  }
  if (payload.type !== 'invoke') {
    send({ type: 'error', payload: { code: 'bad_message', message: 'expected invoke' } }, message.correlation_id ?? null);
    continue;
  }
  const request = payload.payload;
  const args = request.args ?? {};
  const result = module.add(args.a.value, args.b.value);
  const correlationId = request.correlation_id ?? null;
  send({ type: 'event', payload: { level: 'info', message: 'node worker invoked', metadata: { state: result.state } } }, correlationId);
  send({
    type: 'response',
    payload: {
      protocol_version: 1,
      correlation_id: correlationId,
      outputs: {
        out: { kind: 'int', value: result.total },
        loads: { kind: 'int', value: result.loads },
      },
      state: { kind: 'int', value: result.state },
      events: [],
    },
  }, correlationId);
}
"#,
        )
        .expect("write node worker");
    worker_path
}

fn write_java_worker(dir: &Path, javac: &str) -> PathBuf {
    let classes = dir.join("classes");
    std::fs::create_dir_all(&classes).expect("create classes dir");
    let module_path = dir.join("DemoModule.java");
    std::fs::write(
        &module_path,
        r#"
public final class DemoModule {
    public static int LOAD_COUNT = 0;
    private static int state = 0;

    static {
        LOAD_COUNT += 1;
    }

    public static int[] add(int a, int b) {
        state += 1;
        return new int[] { a + b, state, LOAD_COUNT };
    }
}
"#,
    )
    .expect("write java module");

    let worker_path = dir.join("Worker.java");
    std::fs::write(
            &worker_path,
            r#"
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Method;

public final class Worker {
    private static void send(String payload, String correlationId) {
        System.out.println("{\"protocol_version\":1,\"correlation_id\":\"" + correlationId + "\",\"payload\":" + payload + "}");
        System.out.flush();
    }

    public static void main(String[] args) throws Exception {
        Class<?> module = Class.forName(args[0]);
        Method method = module.getMethod(args[1], int.class, int.class);
        send("{\"type\":\"hello\",\"payload\":{\"protocol_version\":1,\"min_protocol_version\":1,\"worker_id\":\"java-test-worker\",\"backend\":\"java\",\"supported_nodes\":[\"demo:add\"],\"capabilities\":[\"persistent_worker\",\"state\",\"events\"]}}", "startup");

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.contains("\"type\":\"ack\"")) {
                continue;
            }
            if (!line.contains("\"type\":\"invoke\"")) {
                send("{\"type\":\"error\",\"payload\":{\"code\":\"bad_message\",\"message\":\"expected invoke\"}}", "req-1");
                continue;
            }
            int[] result = (int[]) method.invoke(null, 2, 40);
            send("{\"type\":\"event\",\"payload\":{\"level\":\"info\",\"message\":\"java worker invoked\",\"metadata\":{\"state\":" + result[1] + "}}}", "req-1");
            send("{\"type\":\"response\",\"payload\":{\"protocol_version\":1,\"correlation_id\":\"req-1\",\"outputs\":{\"out\":{\"kind\":\"int\",\"value\":" + result[0] + "},\"loads\":{\"kind\":\"int\",\"value\":" + result[2] + "}},\"state\":{\"kind\":\"int\",\"value\":" + result[1] + "},\"events\":[]}}", "req-1");
        }
    }
}
"#,
        )
        .expect("write java worker");

    let status = std::process::Command::new(javac)
        .arg("-d")
        .arg(&classes)
        .arg(&module_path)
        .arg(&worker_path)
        .status()
        .expect("spawn javac");
    assert!(status.success(), "javac failed for Java persistent fixture");
    classes
}

fn backend_json_name(backend: &BackendKind) -> &'static str {
    match backend {
        BackendKind::Python => "python",
        BackendKind::Node => "node",
        BackendKind::Java => "java",
        _ => panic!("restart fixture only covers subprocess worker backends"),
    }
}

fn write_one_shot_worker(dir: &Path, backend: &BackendKind, worker_id: &str) -> PathBuf {
    let script = dir.join(format!("{worker_id}.sh"));
    let backend = backend_json_name(backend);
    std::fs::write(
            &script,
            format!(
                r#"#!/bin/sh
printf '%s\n' '{{"protocol_version":1,"correlation_id":"startup","payload":{{"type":"hello","payload":{{"protocol_version":1,"min_protocol_version":1,"worker_id":"{worker_id}","backend":"{backend}","supported_nodes":["demo:add"],"capabilities":["persistent_worker","restart"]}}}}}}'
while IFS= read -r line; do
  case "$line" in
    *'"type":"ack"'*) continue ;;
  esac
  printf '%s\n' '{{"protocol_version":1,"correlation_id":"req-1","payload":{{"type":"event","payload":{{"level":"info","message":"restart fixture invoked","metadata":{{"worker_id":"{worker_id}"}}}}}}}}'
  printf '%s\n' '{{"protocol_version":1,"correlation_id":"req-1","payload":{{"type":"response","payload":{{"protocol_version":1,"correlation_id":"req-1","outputs":{{"out":{{"kind":"int","value":42}}}},"events":[]}}}}}}'
  exit 0
done
"#
            ),
        )
        .expect("write one-shot worker");
    script
}

fn write_python_payload_worker(dir: &Path) -> PathBuf {
    let worker_path = dir.join("payload_worker.py");
    std::fs::write(
            &worker_path,
            r#"
import json
import mmap
import os
import sys

def send(payload, correlation_id=None):
    sys.stdout.write(json.dumps({
        "protocol_version": 1,
        "correlation_id": correlation_id,
        "payload": payload,
    }) + "\n")
    sys.stdout.flush()

send({
    "type": "hello",
    "payload": {
        "protocol_version": 1,
        "min_protocol_version": 1,
        "worker_id": "python-payload-worker",
        "backend": "python",
        "supported_nodes": ["payload:len", "payload:zero_copy_len", "payload:shared_ref_len", "payload:cow_append_marker", "payload:mutable_brighten", "payload:owned_bytes_len"],
        "capabilities": ["persistent_worker", "payload_handle", "memoryview", "mmap"],
    },
}, "startup")

for line in sys.stdin:
    message = json.loads(line)
    payload = message["payload"]
    if payload["type"] == "ack":
        continue
    request = payload["payload"]
    handle = request["args"]["payload"]["value"]
    meta = handle["metadata"]
    correlation_id = request.get("correlation_id")
    node_id = request.get("node_id")
    if not os.path.exists(meta["mmap_path"]):
        send({"type": "error", "payload": {"code": "payload_lease_expired", "message": "payload backing is missing"}}, correlation_id)
        continue
    if node_id == "payload:mutable_brighten":
        with open(meta["mmap_path"], "r+b") as backing:
            mm = mmap.mmap(backing.fileno(), 0, access=mmap.ACCESS_WRITE)
            mm[0] = (mm[0] + 1) % 256
            first = int(mm[0])
            length = meta["mmap_len"]
            mm.flush()
            mm.close()
        send({"type": "response", "payload": {"protocol_version": 1, "correlation_id": correlation_id, "outputs": {"len": {"kind": "int", "value": length}, "first": {"kind": "int", "value": first}}, "events": []}}, correlation_id)
        continue
    with open(meta["mmap_path"], "rb") as backing:
        mm = mmap.mmap(backing.fileno(), 0, access=mmap.ACCESS_READ)
        view = memoryview(mm)[meta.get("mmap_offset", 0):meta["mmap_len"]]
        first = int(view[0])
        length = len(view)
        last = int(view[-1])
        if node_id == "payload:cow_append_marker":
            copy = bytes(view) + bytes([99])
            length = len(copy)
            last = copy[-1]
        view.release()
        mm.close()
    if node_id == "payload:owned_bytes_len":
        os.remove(meta["mmap_path"])
    send({
        "type": "response",
        "payload": {
            "protocol_version": 1,
            "correlation_id": correlation_id,
            "outputs": {
                "len": {"kind": "int", "value": length},
                "first": {"kind": "int", "value": first},
                "last": {"kind": "int", "value": last}
            },
            "events": []
        }
    }, correlation_id)
"#,
        )
        .expect("write python payload worker");
    worker_path
}

fn write_node_payload_worker(dir: &Path) -> PathBuf {
    let worker_path = dir.join("payload_worker.mjs");
    std::fs::write(
            &worker_path,
            r#"
import fs from 'node:fs';
import readline from 'node:readline';

function send(payload, correlationId = null) {
  process.stdout.write(JSON.stringify({
    protocol_version: 1,
    correlation_id: correlationId,
    payload,
  }) + '\n');
}

send({
  type: 'hello',
  payload: {
    protocol_version: 1,
    min_protocol_version: 1,
    worker_id: 'node-payload-worker',
    backend: 'node',
    supported_nodes: ['payload:len', 'payload:zero_copy_len', 'payload:shared_ref_len', 'payload:cow_append_marker', 'payload:mutable_brighten', 'payload:owned_bytes_len'],
    capabilities: ['persistent_worker', 'payload_handle', 'buffer'],
  },
}, 'startup');

const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
for await (const line of rl) {
  const message = JSON.parse(line);
  const payload = message.payload;
  if (payload.type === 'ack') {
    continue;
  }
  const request = payload.payload;
  const handle = request.args.payload.value;
  const meta = handle.metadata;
  const path = meta.shared_memory_name ?? meta.mmap_path;
  const correlationId = request.correlation_id ?? null;
  if (!fs.existsSync(path)) {
    send({ type: 'error', payload: { code: 'payload_lease_expired', message: 'payload backing is missing' } }, correlationId);
    continue;
  }
  const buffer = fs.readFileSync(path);
  const offset = meta.shared_memory_offset ?? meta.mmap_offset ?? 0;
  const len = meta.shared_memory_len ?? meta.mmap_len ?? buffer.length;
  const view = buffer.subarray(offset, offset + len);
  if (request.node_id === 'payload:mutable_brighten') {
    view[0] = (view[0] + 1) & 255;
    fs.writeFileSync(path, buffer);
  }
  const cow = request.node_id === 'payload:cow_append_marker'
    ? Buffer.concat([view, Buffer.from([99])])
    : view;
  if (request.node_id === 'payload:owned_bytes_len') {
    fs.unlinkSync(path);
  }
  send({
    type: 'response',
    payload: {
      protocol_version: 1,
      correlation_id: correlationId,
      outputs: {
        len: { kind: 'int', value: cow.length },
        first: { kind: 'int', value: cow[0] },
        last: { kind: 'int', value: cow[cow.length - 1] },
      },
      events: [],
    },
  }, correlationId);
}
"#,
        )
        .expect("write node payload worker");
    worker_path
}

fn write_java_payload_worker(dir: &Path, javac: &str) -> PathBuf {
    let classes = dir.join("payload_classes");
    std::fs::create_dir_all(&classes).expect("create payload classes");
    let worker_path = dir.join("PayloadWorker.java");
    std::fs::write(
            &worker_path,
            r#"
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public final class PayloadWorker {
    private static void send(String payload, String correlationId) {
        System.out.println("{\"protocol_version\":1,\"correlation_id\":\"" + correlationId + "\",\"payload\":" + payload + "}");
        System.out.flush();
    }

    private static String stringField(String line, String key) {
        String marker = "\"" + key + "\":\"";
        int start = line.indexOf(marker);
        if (start < 0) {
            return "";
        }
        start += marker.length();
        int end = line.indexOf("\"", start);
        return line.substring(start, end);
    }

    private static long longField(String line, String key) {
        String marker = "\"" + key + "\":";
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
        send("{\"type\":\"hello\",\"payload\":{\"protocol_version\":1,\"min_protocol_version\":1,\"worker_id\":\"java-payload-worker\",\"backend\":\"java\",\"supported_nodes\":[\"payload:len\",\"payload:zero_copy_len\",\"payload:shared_ref_len\",\"payload:cow_append_marker\",\"payload:mutable_brighten\",\"payload:owned_bytes_len\"],\"capabilities\":[\"persistent_worker\",\"payload_handle\",\"direct_byte_buffer\",\"mmap\"]}}", "startup");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.contains("\"type\":\"ack\"")) {
                continue;
            }
            String nodeId = stringField(line, "node_id");
            String path = stringField(line, "mmap_path");
            long len = longField(line, "mmap_len");
            if (!new File(path).exists()) {
                send("{\"type\":\"error\",\"payload\":{\"code\":\"payload_lease_expired\",\"message\":\"payload backing is missing\"}}", "payload-req-1");
                continue;
            }
            if (nodeId.equals("payload:mutable_brighten")) {
                try (RandomAccessFile file = new RandomAccessFile(path, "rw")) {
                    MappedByteBuffer buffer = file.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, len);
                    int first = (Byte.toUnsignedInt(buffer.get(0)) + 1) & 255;
                    buffer.put(0, (byte) first);
                    send("{\"type\":\"response\",\"payload\":{\"protocol_version\":1,\"correlation_id\":\"payload-req-1\",\"outputs\":{\"len\":{\"kind\":\"int\",\"value\":" + len + "},\"first\":{\"kind\":\"int\",\"value\":" + first + "}},\"events\":[]}}", "payload-req-1");
                }
                continue;
            }
            try (RandomAccessFile file = new RandomAccessFile(path, "r")) {
                MappedByteBuffer buffer = file.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, len);
                int first = Byte.toUnsignedInt(buffer.get(0));
                int last = nodeId.equals("payload:cow_append_marker") ? 99 : Byte.toUnsignedInt(buffer.get((int) len - 1));
                long outLen = nodeId.equals("payload:cow_append_marker") ? len + 1 : len;
                if (nodeId.equals("payload:owned_bytes_len")) {
                    new File(path).delete();
                }
                send("{\"type\":\"response\",\"payload\":{\"protocol_version\":1,\"correlation_id\":\"payload-req-1\",\"outputs\":{\"len\":{\"kind\":\"int\",\"value\":" + outLen + "},\"first\":{\"kind\":\"int\",\"value\":" + first + "},\"last\":{\"kind\":\"int\",\"value\":" + last + "}},\"events\":[]}}", "payload-req-1");
            }
        }
    }
}
"#,
        )
        .expect("write java payload worker");
    let status = std::process::Command::new(javac)
        .arg("-d")
        .arg(&classes)
        .arg(&worker_path)
        .status()
        .expect("spawn javac");
    assert!(status.success(), "javac failed for Java payload fixture");
    classes
}

fn one_shot_worker_config(backend: BackendKind, worker: &Path, dir: &Path) -> BackendConfig {
    let (entry_module, entry_class) = match backend {
        BackendKind::Python => (Some("restart_fixture.py".into()), None),
        BackendKind::Node => (Some("restart_fixture.mjs".into()), None),
        BackendKind::Java => (None, Some("RestartFixture".into())),
        _ => panic!("restart fixture only covers subprocess worker backends"),
    };
    BackendConfig {
        backend,
        runtime_model: BackendRuntimeModel::PersistentWorker,
        entry_module,
        entry_class,
        entry_symbol: Some("add".into()),
        executable: Some("/bin/sh".into()),
        args: vec![worker.display().to_string()],
        classpath: Vec::new(),
        native_library_paths: Vec::new(),
        working_dir: Some(dir.display().to_string()),
        env: BTreeMap::new(),
        options: BTreeMap::new(),
    }
}

fn wait_for_stopped(runner: &PersistentWorkerRunner) {
    for _ in 0..50 {
        if runner.health() == RunnerHealth::Stopped {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(runner.health(), RunnerHealth::Stopped);
}

fn assert_payload_ownership_modes(runner: &PersistentWorkerRunner, dir: &Path) {
    let zero_copy = dir.join("zero-copy.bin");
    std::fs::write(&zero_copy, [21_u8, 2, 3, 4]).expect("write zero-copy payload");
    let response = runner
        .invoke(payload_mode_request(
            &zero_copy,
            4,
            "payload:zero_copy_len",
            AccessMode::View,
        ))
        .expect("zero-copy invoke");
    assert_eq!(response.outputs.get("len"), Some(&WireValue::Int(4)));
    assert_eq!(response.outputs.get("first"), Some(&WireValue::Int(21)));
    assert_eq!(
        std::fs::read(&zero_copy).expect("read zero-copy payload"),
        vec![21_u8, 2, 3, 4]
    );

    let shared = dir.join("shared.bin");
    std::fs::write(&shared, [22_u8, 2, 3, 4]).expect("write shared payload");
    let response = runner
        .invoke(payload_mode_request(
            &shared,
            4,
            "payload:shared_ref_len",
            AccessMode::Read,
        ))
        .expect("shared invoke");
    assert_eq!(response.outputs.get("len"), Some(&WireValue::Int(4)));
    assert_eq!(response.outputs.get("first"), Some(&WireValue::Int(22)));
    assert_eq!(
        std::fs::read(&shared).expect("read shared payload"),
        vec![22_u8, 2, 3, 4]
    );

    let cow = dir.join("cow.bin");
    std::fs::write(&cow, [23_u8, 2, 3, 4]).expect("write cow payload");
    let response = runner
        .invoke(payload_mode_request(
            &cow,
            4,
            "payload:cow_append_marker",
            AccessMode::Modify,
        ))
        .expect("cow invoke");
    assert_eq!(response.outputs.get("len"), Some(&WireValue::Int(5)));
    assert_eq!(response.outputs.get("first"), Some(&WireValue::Int(23)));
    assert_eq!(response.outputs.get("last"), Some(&WireValue::Int(99)));
    assert_eq!(
        std::fs::read(&cow).expect("read cow payload"),
        vec![23_u8, 2, 3, 4]
    );

    let mutable = dir.join("mutable.bin");
    std::fs::write(&mutable, [24_u8, 2, 3, 4]).expect("write mutable payload");
    let response = runner
        .invoke(payload_mode_request(
            &mutable,
            4,
            "payload:mutable_brighten",
            AccessMode::Modify,
        ))
        .expect("mutable invoke");
    assert_eq!(response.outputs.get("len"), Some(&WireValue::Int(4)));
    assert_eq!(response.outputs.get("first"), Some(&WireValue::Int(25)));
    assert_eq!(
        std::fs::read(&mutable).expect("read mutable payload"),
        vec![25_u8, 2, 3, 4]
    );

    let owned = dir.join("owned.bin");
    std::fs::write(&owned, [26_u8, 2, 3, 4]).expect("write owned payload");
    let response = runner
        .invoke(payload_mode_request(
            &owned,
            4,
            "payload:owned_bytes_len",
            AccessMode::Move,
        ))
        .expect("owned invoke");
    assert_eq!(response.outputs.get("len"), Some(&WireValue::Int(4)));
    assert_eq!(response.outputs.get("first"), Some(&WireValue::Int(26)));
    assert!(
        !owned.exists(),
        "owned/move payload backing should be consumed by the worker"
    );
}

fn assert_payload_ownership_telemetry(telemetry: &FfiHostTelemetry) {
    let report = telemetry.snapshot();
    assert_eq!(report.payloads.handles_resolved, 5);
    assert_eq!(report.payloads.zero_copy_hits, 1);
    assert_eq!(report.payloads.shared_reference_hits, 1);
    assert_eq!(report.payloads.cow_materializations, 1);
    assert_eq!(report.payloads.mutable_in_place_hits, 1);
    assert_eq!(report.payloads.owned_moves, 1);
    assert_eq!(report.payloads.by_access_mode.get("view"), Some(&1));
    assert_eq!(report.payloads.by_access_mode.get("read"), Some(&1));
    assert_eq!(report.payloads.by_access_mode.get("modify"), Some(&2));
    assert_eq!(report.payloads.by_access_mode.get("move"), Some(&1));
}

mod lifecycle;
mod payload;
mod persistent;
