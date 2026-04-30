use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::Mutex;

use daedalus_ffi_core::{
    BackendConfig, BackendRuntimeModel, InvokeEvent, InvokeRequest, InvokeResponse, WorkerError,
    WorkerHello, WorkerMessage, WorkerMessagePayload, WorkerProtocolAck, WorkerProtocolError,
};

use crate::{BackendRunner, RunnerHealth, RunnerLimits, RunnerPoolError};

#[derive(Debug)]
pub struct PersistentWorkerRunner {
    executable: String,
    args: Vec<String>,
    env: BTreeMap<String, String>,
    working_dir: Option<String>,
    stderr_capture_bytes: usize,
    process: Mutex<Option<PersistentWorkerProcess>>,
    hello: Mutex<Option<WorkerHello>>,
}

#[derive(Debug)]
struct PersistentWorkerProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    stderr: Option<ChildStderr>,
}

impl PersistentWorkerRunner {
    pub fn from_backend(config: &BackendConfig) -> Result<Self, RunnerPoolError> {
        Self::from_backend_with_limits(config, &RunnerLimits::default())
    }

    pub fn from_backend_with_limits(
        config: &BackendConfig,
        limits: &RunnerLimits,
    ) -> Result<Self, RunnerPoolError> {
        if config.runtime_model != BackendRuntimeModel::PersistentWorker {
            return Err(RunnerPoolError::Runner(format!(
                "persistent worker runner requires persistent_worker backend config, found {:?}",
                config.runtime_model
            )));
        }
        let executable = config.executable.clone().ok_or_else(|| {
            RunnerPoolError::Runner("persistent worker executable missing".into())
        })?;
        Ok(Self {
            executable,
            args: config.args.clone(),
            env: config.env.clone(),
            working_dir: config.working_dir.clone(),
            stderr_capture_bytes: limits.stderr_capture_bytes,
            process: Mutex::new(None),
            hello: Mutex::new(None),
        })
    }

    pub fn executable(&self) -> &str {
        &self.executable
    }

    pub fn hello(&self) -> Option<WorkerHello> {
        self.hello.lock().ok().and_then(|hello| hello.clone())
    }

    fn spawn_process(&self) -> Result<PersistentWorkerProcess, RunnerPoolError> {
        let mut command = Command::new(&self.executable);
        command
            .args(&self.args)
            .envs(&self.env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(working_dir) = &self.working_dir {
            command.current_dir(working_dir);
        }

        let mut child = command.spawn().map_err(|err| {
            RunnerPoolError::Runner(format!("failed to spawn persistent worker: {err}"))
        })?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| RunnerPoolError::Runner("persistent worker stdin unavailable".into()))?;
        let stdout = child.stdout.take().ok_or_else(|| {
            RunnerPoolError::Runner("persistent worker stdout unavailable".into())
        })?;
        let stderr = child.stderr.take();
        Ok(PersistentWorkerProcess {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            stderr,
        })
    }

    fn write_message(
        process: &mut PersistentWorkerProcess,
        message: &WorkerMessage,
    ) -> Result<(), RunnerPoolError> {
        serde_json::to_writer(&mut process.stdin, message).map_err(|err| {
            RunnerPoolError::Runner(format!("failed to encode worker message: {err}"))
        })?;
        process.stdin.write_all(b"\n").map_err(|err| {
            RunnerPoolError::Runner(format!("failed to write worker message: {err}"))
        })?;
        process.stdin.flush().map_err(|err| {
            RunnerPoolError::Runner(format!("failed to flush worker message: {err}"))
        })
    }

    fn read_message(
        &self,
        process: &mut PersistentWorkerProcess,
    ) -> Result<WorkerMessage, RunnerPoolError> {
        let mut line = String::new();
        let bytes = process.stdout.read_line(&mut line).map_err(|err| {
            RunnerPoolError::Runner(format!("failed to read worker message: {err}"))
        })?;
        if bytes == 0 {
            let stderr = read_capped_stderr(&mut process.stderr, self.stderr_capture_bytes);
            return Err(RunnerPoolError::Runner(format!(
                "persistent worker stdout closed: {stderr}"
            )));
        }
        let message: WorkerMessage = serde_json::from_str(line.trim_end()).map_err(|err| {
            RunnerPoolError::Runner(format!("failed to decode worker message: {err}"))
        })?;
        message.validate_protocol().map_err(worker_protocol_error)?;
        Ok(message)
    }

    fn start_locked(
        &self,
        slot: &mut Option<PersistentWorkerProcess>,
    ) -> Result<(), RunnerPoolError> {
        let mut process = self.spawn_process()?;
        let hello_message = self.read_message(&mut process)?;
        let WorkerMessagePayload::Hello(hello) = hello_message.payload else {
            return Err(RunnerPoolError::Runner(
                "persistent worker first message was not hello".into(),
            ));
        };
        let ack = WorkerProtocolAck::from_hello(&hello).map_err(worker_protocol_error)?;
        Self::write_message(
            &mut process,
            &WorkerMessage::new(
                WorkerMessagePayload::Ack(ack),
                hello_message.correlation_id.clone(),
            ),
        )?;
        *self
            .hello
            .lock()
            .map_err(|_| RunnerPoolError::LockPoisoned)? = Some(hello);
        *slot = Some(process);
        Ok(())
    }

    fn stop_locked(slot: &mut Option<PersistentWorkerProcess>) {
        if let Some(mut process) = slot.take() {
            let _ = process.child.kill();
            let _ = process.child.wait();
        }
    }
}

impl BackendRunner for PersistentWorkerRunner {
    fn start(&self) -> Result<(), RunnerPoolError> {
        let mut slot = self
            .process
            .lock()
            .map_err(|_| RunnerPoolError::LockPoisoned)?;
        if let Some(process) = slot.as_mut() {
            match process.child.try_wait() {
                Ok(None) => return Ok(()),
                Ok(Some(_)) | Err(_) => Self::stop_locked(&mut slot),
            }
        }
        self.start_locked(&mut slot)
    }

    fn health(&self) -> RunnerHealth {
        let Ok(mut slot) = self.process.lock() else {
            return RunnerHealth::Degraded;
        };
        let Some(process) = slot.as_mut() else {
            return RunnerHealth::Starting;
        };
        match process.child.try_wait() {
            Ok(Some(_)) => RunnerHealth::Stopped,
            Ok(None) => RunnerHealth::Ready,
            Err(_) => RunnerHealth::Degraded,
        }
    }

    fn supported_nodes(&self) -> Option<Vec<String>> {
        self.hello().map(|hello| hello.supported_nodes)
    }

    fn invoke(&self, request: InvokeRequest) -> Result<InvokeResponse, RunnerPoolError> {
        request.validate_protocol().map_err(worker_protocol_error)?;
        let mut slot = self
            .process
            .lock()
            .map_err(|_| RunnerPoolError::LockPoisoned)?;
        let process = slot
            .as_mut()
            .ok_or_else(|| RunnerPoolError::Runner("persistent worker has not started".into()))?;
        let correlation_id = request.correlation_id.clone();
        Self::write_message(
            process,
            &WorkerMessage::new(
                WorkerMessagePayload::Invoke(request),
                correlation_id.clone(),
            ),
        )?;

        let mut events = Vec::<InvokeEvent>::new();
        loop {
            let message = self.read_message(process)?;
            match message.payload {
                WorkerMessagePayload::Event(event) => events.push(event),
                WorkerMessagePayload::Response(mut response) => {
                    if response.events.is_empty() {
                        response.events = events;
                    } else if !events.is_empty() {
                        events.extend(response.events);
                        response.events = events;
                    }
                    response
                        .validate_protocol()
                        .map_err(worker_protocol_error)?;
                    return Ok(response);
                }
                WorkerMessagePayload::Error(error) => return Err(worker_error(error)),
                other => {
                    return Err(RunnerPoolError::Runner(format!(
                        "unexpected worker message while awaiting response: {other:?}"
                    )));
                }
            }
        }
    }

    fn shutdown(&self) -> Result<(), RunnerPoolError> {
        let mut slot = self
            .process
            .lock()
            .map_err(|_| RunnerPoolError::LockPoisoned)?;
        Self::stop_locked(&mut slot);
        Ok(())
    }
}

fn worker_protocol_error(err: WorkerProtocolError) -> RunnerPoolError {
    RunnerPoolError::Runner(format!("worker protocol error: {err}"))
}

fn worker_error(err: WorkerError) -> RunnerPoolError {
    RunnerPoolError::Runner(format!("worker error {}: {}", err.code, err.message))
}

fn capped_stderr(stderr: &[u8], limit: usize) -> String {
    let len = stderr.len().min(limit);
    let mut text = String::from_utf8_lossy(&stderr[..len]).into_owned();
    if stderr.len() > limit {
        text.push_str("...");
    }
    text
}

fn read_capped_stderr(stderr: &mut Option<ChildStderr>, limit: usize) -> String {
    let Some(stderr) = stderr.as_mut() else {
        return String::new();
    };
    let mut bytes = Vec::new();
    match stderr.take((limit + 1) as u64).read_to_end(&mut bytes) {
        Ok(_) => capped_stderr(&bytes, limit),
        Err(err) => format!("failed to read stderr: {err}"),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use daedalus_ffi_core::{
        BackendConfig, BackendKind, BackendRuntimeModel, InvokeEventLevel, InvokeRequest,
        WORKER_PROTOCOL_VERSION, WireValue,
    };

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

    #[test]
    fn persistent_python_worker_loads_module_once_and_invokes_repeatedly() {
        let Some(python) = python_available() else {
            eprintln!("skipping: python interpreter not found");
            return;
        };
        let dir = temp_dir("persistent_python_worker");
        let worker = write_python_worker(&dir);
        let module = dir.join("demo_module.py");
        let config = BackendConfig {
            backend: BackendKind::Python,
            runtime_model: BackendRuntimeModel::PersistentWorker,
            entry_module: Some(module.display().to_string()),
            entry_class: None,
            entry_symbol: Some("add".into()),
            executable: Some(python),
            args: vec![worker.display().to_string(), module.display().to_string()],
            classpath: Vec::new(),
            native_library_paths: Vec::new(),
            working_dir: Some(dir.display().to_string()),
            env: BTreeMap::new(),
            options: BTreeMap::new(),
        };
        let runner = PersistentWorkerRunner::from_backend(&config).expect("runner");

        runner.start().expect("start");
        let hello = runner.hello().expect("hello");
        assert_eq!(hello.worker_id.as_deref(), Some("python-test-worker"));
        assert_eq!(runner.supported_nodes(), Some(vec!["demo:add".into()]));

        let first = runner.invoke(request()).expect("first invoke");
        let second = runner.invoke(request()).expect("second invoke");

        assert_eq!(first.outputs.get("out"), Some(&WireValue::Int(42)));
        assert_eq!(second.outputs.get("out"), Some(&WireValue::Int(42)));
        assert_eq!(first.outputs.get("loads"), Some(&WireValue::Int(1)));
        assert_eq!(second.outputs.get("loads"), Some(&WireValue::Int(1)));
        assert_eq!(first.state, Some(WireValue::Int(1)));
        assert_eq!(second.state, Some(WireValue::Int(2)));
        assert_eq!(second.events.len(), 1);
        assert_eq!(second.events[0].level, InvokeEventLevel::Info);
        runner.shutdown().expect("shutdown");
    }

    #[test]
    fn persistent_node_worker_imports_module_once_and_invokes_repeatedly() {
        let Some(node) = node_available() else {
            eprintln!("skipping: node executable not found");
            return;
        };
        let dir = temp_dir("persistent_node_worker");
        let worker = write_node_worker(&dir);
        let module = dir.join("demo_module.mjs");
        let config = BackendConfig {
            backend: BackendKind::Node,
            runtime_model: BackendRuntimeModel::PersistentWorker,
            entry_module: Some(module.display().to_string()),
            entry_class: None,
            entry_symbol: Some("add".into()),
            executable: Some(node),
            args: vec![worker.display().to_string(), module.display().to_string()],
            classpath: Vec::new(),
            native_library_paths: Vec::new(),
            working_dir: Some(dir.display().to_string()),
            env: BTreeMap::new(),
            options: BTreeMap::new(),
        };
        let runner = PersistentWorkerRunner::from_backend(&config).expect("runner");

        runner.start().expect("start");
        let hello = runner.hello().expect("hello");
        assert_eq!(hello.worker_id.as_deref(), Some("node-test-worker"));
        assert_eq!(runner.supported_nodes(), Some(vec!["demo:add".into()]));

        let first = runner.invoke(request()).expect("first invoke");
        let second = runner.invoke(request()).expect("second invoke");

        assert_eq!(first.outputs.get("out"), Some(&WireValue::Int(42)));
        assert_eq!(second.outputs.get("out"), Some(&WireValue::Int(42)));
        assert_eq!(first.outputs.get("loads"), Some(&WireValue::Int(1)));
        assert_eq!(second.outputs.get("loads"), Some(&WireValue::Int(1)));
        assert_eq!(first.state, Some(WireValue::Int(1)));
        assert_eq!(second.state, Some(WireValue::Int(2)));
        assert_eq!(second.events.len(), 1);
        assert_eq!(second.events[0].level, InvokeEventLevel::Info);
        runner.shutdown().expect("shutdown");
    }

    #[test]
    fn persistent_java_worker_loads_classpath_once_and_invokes_repeatedly() {
        let Some((javac, java)) = java_available() else {
            eprintln!("skipping: java/javac not found");
            return;
        };
        let dir = temp_dir("persistent_java_worker");
        let classes = write_java_worker(&dir, &javac);
        let config = BackendConfig {
            backend: BackendKind::Java,
            runtime_model: BackendRuntimeModel::PersistentWorker,
            entry_module: None,
            entry_class: Some("DemoModule".into()),
            entry_symbol: Some("add".into()),
            executable: Some(java),
            args: vec![
                "-cp".into(),
                classes.display().to_string(),
                "Worker".into(),
                "DemoModule".into(),
                "add".into(),
            ],
            classpath: vec![classes.display().to_string()],
            native_library_paths: Vec::new(),
            working_dir: Some(dir.display().to_string()),
            env: BTreeMap::new(),
            options: BTreeMap::new(),
        };
        let runner = PersistentWorkerRunner::from_backend(&config).expect("runner");

        runner.start().expect("start");
        let hello = runner.hello().expect("hello");
        assert_eq!(hello.worker_id.as_deref(), Some("java-test-worker"));
        assert_eq!(runner.supported_nodes(), Some(vec!["demo:add".into()]));

        let first = runner.invoke(request()).expect("first invoke");
        let second = runner.invoke(request()).expect("second invoke");

        assert_eq!(first.outputs.get("out"), Some(&WireValue::Int(42)));
        assert_eq!(second.outputs.get("out"), Some(&WireValue::Int(42)));
        assert_eq!(first.outputs.get("loads"), Some(&WireValue::Int(1)));
        assert_eq!(second.outputs.get("loads"), Some(&WireValue::Int(1)));
        assert_eq!(first.state, Some(WireValue::Int(1)));
        assert_eq!(second.state, Some(WireValue::Int(2)));
        assert_eq!(second.events.len(), 1);
        assert_eq!(second.events[0].level, InvokeEventLevel::Info);
        runner.shutdown().expect("shutdown");
    }

    #[test]
    fn persistent_worker_restarts_exited_python_node_and_java_backends() {
        for (backend, worker_id) in [
            (BackendKind::Python, "python-restart-worker"),
            (BackendKind::Node, "node-restart-worker"),
            (BackendKind::Java, "java-restart-worker"),
        ] {
            let dir = temp_dir(worker_id);
            let worker = write_one_shot_worker(&dir, &backend, worker_id);
            let config = one_shot_worker_config(backend.clone(), &worker, &dir);
            let runner = PersistentWorkerRunner::from_backend(&config).expect("runner");

            runner.start().expect("first start");
            let first_hello = runner.hello().expect("first hello");
            assert_eq!(first_hello.worker_id.as_deref(), Some(worker_id));
            assert_eq!(first_hello.backend, Some(backend.clone()));
            let first = runner.invoke(request()).expect("first invoke");
            assert_eq!(first.outputs.get("out"), Some(&WireValue::Int(42)));
            assert_eq!(first.events.len(), 1);

            wait_for_stopped(&runner);
            runner.start().expect("restart");
            let second_hello = runner.hello().expect("second hello");
            assert_eq!(second_hello.worker_id.as_deref(), Some(worker_id));
            assert_eq!(second_hello.backend, Some(backend));
            let second = runner.invoke(request()).expect("second invoke");
            assert_eq!(second.outputs.get("out"), Some(&WireValue::Int(42)));
            assert_eq!(second.events.len(), 1);

            runner.shutdown().expect("shutdown");
        }
    }

    #[test]
    fn persistent_worker_reports_crash_and_malformed_messages() {
        let crash = PersistentWorkerRunner::from_backend(&BackendConfig {
            backend: BackendKind::Python,
            runtime_model: BackendRuntimeModel::PersistentWorker,
            entry_module: Some("crash".into()),
            entry_class: None,
            entry_symbol: Some("run".into()),
            executable: Some("/bin/sh".into()),
            args: vec![
                "-c".into(),
                "printf 'worker failed with a long diagnostic' >&2; exit 9".into(),
            ],
            classpath: Vec::new(),
            native_library_paths: Vec::new(),
            working_dir: None,
            env: BTreeMap::new(),
            options: BTreeMap::new(),
        })
        .expect("crash runner");
        assert!(matches!(
            crash.start(),
            Err(RunnerPoolError::Runner(message))
                if message.contains("stdout closed")
                    && message.contains("worker failed with a long diagnostic")
        ));

        let malformed = PersistentWorkerRunner::from_backend(&BackendConfig {
            backend: BackendKind::Python,
            runtime_model: BackendRuntimeModel::PersistentWorker,
            entry_module: Some("bad".into()),
            entry_class: None,
            entry_symbol: Some("run".into()),
            executable: Some("/bin/sh".into()),
            args: vec![
                "-c".into(),
                "printf 'not-json\\n'; while read line; do :; done".into(),
            ],
            classpath: Vec::new(),
            native_library_paths: Vec::new(),
            working_dir: None,
            env: BTreeMap::new(),
            options: BTreeMap::new(),
        })
        .expect("malformed runner");
        assert!(matches!(
            malformed.start(),
            Err(RunnerPoolError::Runner(message))
                if message.contains("failed to decode worker message")
        ));
    }
}
