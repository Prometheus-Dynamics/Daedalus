use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::fd::AsRawFd;
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use daedalus_ffi_core::{
    BackendConfig, BackendRuntimeModel, InvokeEvent, InvokeRequest, InvokeResponse, WorkerError,
    WorkerHello, WorkerMessage, WorkerMessagePayload, WorkerProtocolAck, WorkerProtocolError,
};
use daedalus_runtime::{FfiPayloadTelemetry, FfiWorkerTelemetry};

use crate::{
    BackendRunner, FfiHostTelemetry, RunnerHealth, RunnerLimits, RunnerPoolError,
    RunnerRestartPolicy,
};

#[derive(Debug)]
pub struct PersistentWorkerRunner {
    executable: String,
    args: Vec<String>,
    env: BTreeMap<String, String>,
    working_dir: Option<String>,
    request_timeout: Option<Duration>,
    stderr_capture_bytes: usize,
    process: Mutex<Option<PersistentWorkerProcess>>,
    hello: Mutex<Option<WorkerHello>>,
    telemetry: Option<FfiHostTelemetry>,
}

#[derive(Debug)]
struct PersistentWorkerProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    stderr: StderrDrain,
}

#[derive(Debug, Default)]
struct StderrCapture {
    bytes: Vec<u8>,
    truncated: bool,
    read_error: Option<String>,
}

#[derive(Debug)]
struct StderrDrain {
    capture: Arc<Mutex<StderrCapture>>,
    handle: Option<JoinHandle<()>>,
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
        validate_persistent_worker_limits(limits)?;
        let executable = config.executable.clone().ok_or_else(|| {
            RunnerPoolError::Runner("persistent worker executable missing".into())
        })?;
        Ok(Self {
            executable,
            args: config.args.clone(),
            env: config.env.clone(),
            working_dir: config.working_dir.clone(),
            request_timeout: limits.request_timeout,
            stderr_capture_bytes: limits.stderr_capture_bytes,
            process: Mutex::new(None),
            hello: Mutex::new(None),
            telemetry: None,
        })
    }

    pub fn from_backend_with_limits_and_telemetry(
        config: &BackendConfig,
        limits: &RunnerLimits,
        telemetry: FfiHostTelemetry,
    ) -> Result<Self, RunnerPoolError> {
        match Self::from_backend_with_limits(config, limits) {
            Ok(runner) => Ok(runner.with_ffi_telemetry(telemetry)),
            Err(err) => {
                if matches!(err, RunnerPoolError::UnsupportedRunnerLimit { .. }) {
                    telemetry.record_worker(
                        worker_id_from_backend(config),
                        FfiWorkerTelemetry {
                            unsupported_limit_errors: 1,
                            timeout_failures: u64::from(limits.request_timeout.is_some()),
                            ..Default::default()
                        },
                    );
                }
                Err(err)
            }
        }
    }

    pub fn with_ffi_telemetry(mut self, telemetry: FfiHostTelemetry) -> Self {
        self.telemetry = Some(telemetry);
        self
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
        let stderr = child
            .stderr
            .take()
            .map(|stderr| StderrDrain::spawn(stderr, self.stderr_capture_bytes));
        Ok(PersistentWorkerProcess {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            stderr: stderr.unwrap_or_default(),
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
        timeout: Option<Duration>,
    ) -> Result<WorkerMessage, RunnerPoolError> {
        if let Some(timeout) = timeout
            && !wait_stdout_readable(&process.stdout, timeout)?
        {
            return Err(RunnerPoolError::RequestTimedOut { timeout });
        }
        let mut line = String::new();
        let bytes = process.stdout.read_line(&mut line).map_err(|err| {
            RunnerPoolError::Runner(format!("failed to read worker message: {err}"))
        })?;
        if bytes == 0 {
            if wait_child_exit(&mut process.child, Duration::from_millis(50)) {
                process.stderr.finish();
            }
            let stderr = process.stderr.snapshot();
            if !stderr.is_empty() {
                self.record_worker_telemetry(FfiWorkerTelemetry {
                    worker_id: self.worker_id(),
                    stderr_events: 1,
                    ..Default::default()
                });
            }
            return Err(RunnerPoolError::Runner(format!(
                "persistent worker stdout closed: {stderr}"
            )));
        }
        let message: WorkerMessage = serde_json::from_str(line.trim_end()).map_err(|err| {
            self.record_worker_telemetry(FfiWorkerTelemetry {
                worker_id: self.worker_id(),
                malformed_responses: 1,
                ..Default::default()
            });
            RunnerPoolError::Runner(format!("failed to decode worker message: {err}"))
        })?;
        message.validate_protocol().map_err(|err| {
            self.record_worker_telemetry(FfiWorkerTelemetry {
                worker_id: self.worker_id(),
                malformed_responses: 1,
                ..Default::default()
            });
            worker_protocol_error(err)
        })?;
        Ok(message)
    }

    fn start_locked(
        &self,
        slot: &mut Option<PersistentWorkerProcess>,
    ) -> Result<(), RunnerPoolError> {
        let handshake_started = Instant::now();
        let mut process = self.spawn_process()?;
        let hello_message = self.read_message(&mut process, None)?;
        let WorkerMessagePayload::Hello(hello) = hello_message.payload else {
            self.record_worker_telemetry(FfiWorkerTelemetry {
                worker_id: self.worker_id(),
                malformed_responses: 1,
                handshake_duration: handshake_started.elapsed(),
                ..Default::default()
            });
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
        self.record_worker_telemetry(FfiWorkerTelemetry {
            worker_id: self.worker_id(),
            handshakes: 1,
            handshake_duration: handshake_started.elapsed(),
            ..Default::default()
        });
        Ok(())
    }

    fn stop_locked(slot: &mut Option<PersistentWorkerProcess>) {
        if let Some(mut process) = slot.take() {
            let _ = process.child.kill();
            let _ = process.child.wait();
            process.stderr.finish();
        }
    }

    fn worker_id(&self) -> String {
        worker_id_from_parts(&self.executable, &self.args)
    }

    fn record_worker_telemetry(&self, mut update: FfiWorkerTelemetry) {
        let Some(telemetry) = &self.telemetry else {
            return;
        };
        if update.worker_id.is_empty() {
            update.worker_id = self.worker_id();
        }
        telemetry.record_worker(update.worker_id.clone(), update);
    }

    fn record_request_payload_telemetry(&self, request: &InvokeRequest) {
        let Some(telemetry) = &self.telemetry else {
            return;
        };
        let mut payloads = FfiPayloadTelemetry::default();
        for value in request.args.values() {
            accumulate_payload_value(value, &mut payloads);
        }
        if !payloads.is_empty() {
            telemetry.record_payloads(payloads);
        }
    }
}

fn wait_child_exit(child: &mut Child, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return true,
            Ok(None) => {
                if Instant::now() >= deadline {
                    return false;
                }
                thread::sleep(Duration::from_millis(1));
            }
            Err(_) => return false,
        }
    }
}

fn validate_persistent_worker_limits(limits: &RunnerLimits) -> Result<(), RunnerPoolError> {
    if limits.queue_depth != 1 {
        return Err(RunnerPoolError::UnsupportedRunnerLimit {
            limit: "queue_depth",
            message: "persistent workers currently serialize calls through one process pipe".into(),
        });
    }
    if limits.restart_policy != RunnerRestartPolicy::Never {
        return Err(RunnerPoolError::UnsupportedRunnerLimit {
            limit: "restart_policy",
            message: "restart policy is managed by explicit start/restart calls for now".into(),
        });
    }
    Ok(())
}

fn worker_id_from_backend(config: &BackendConfig) -> String {
    worker_id_from_parts(
        config.executable.as_deref().unwrap_or("<missing>"),
        &config.args,
    )
}

fn worker_id_from_parts(executable: &str, args: &[String]) -> String {
    if args.is_empty() {
        executable.to_owned()
    } else {
        format!("{} {}", executable, args.join(" "))
    }
}

fn wait_stdout_readable(
    stdout: &BufReader<ChildStdout>,
    timeout: Duration,
) -> Result<bool, RunnerPoolError> {
    if !stdout.buffer().is_empty() {
        return Ok(true);
    }
    let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as i32;
    let mut fd = libc::pollfd {
        fd: stdout.get_ref().as_raw_fd(),
        events: libc::POLLIN | libc::POLLHUP | libc::POLLERR,
        revents: 0,
    };
    loop {
        // SAFETY: `fd` contains a valid descriptor owned by `ChildStdout`, and the pointer/count
        // describe exactly one initialized `pollfd` for the duration of this call.
        let result = unsafe { libc::poll(&mut fd, 1, timeout_ms) };
        if result > 0 {
            return Ok(true);
        }
        if result == 0 {
            return Ok(false);
        }
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        return Err(RunnerPoolError::Runner(format!(
            "failed to wait for worker stdout: {err}"
        )));
    }
}

fn accumulate_payload_value(
    value: &daedalus_ffi_core::WireValue,
    payloads: &mut FfiPayloadTelemetry,
) {
    match value {
        daedalus_ffi_core::WireValue::Handle(handle) => {
            payloads.handles_resolved = payloads.handles_resolved.saturating_add(1);
            payloads.borrows = payloads.borrows.saturating_add(1);
            match handle.access {
                daedalus_transport::AccessMode::View => {
                    payloads.zero_copy_hits = payloads.zero_copy_hits.saturating_add(1);
                }
                daedalus_transport::AccessMode::Read => {
                    payloads.shared_reference_hits =
                        payloads.shared_reference_hits.saturating_add(1);
                }
                daedalus_transport::AccessMode::Modify if is_cow_payload_handle(handle) => {
                    payloads.cow_materializations = payloads.cow_materializations.saturating_add(1);
                }
                daedalus_transport::AccessMode::Modify => {
                    payloads.mutable_in_place_hits =
                        payloads.mutable_in_place_hits.saturating_add(1);
                }
                daedalus_transport::AccessMode::Move => {
                    payloads.owned_moves = payloads.owned_moves.saturating_add(1);
                }
            }
            payloads
                .by_access_mode
                .entry(handle.access.as_str().to_owned())
                .and_modify(|count| *count = count.saturating_add(1))
                .or_insert(1);
            if let Some(residency) = handle.residency {
                payloads
                    .by_residency
                    .entry(residency.as_str().to_owned())
                    .and_modify(|count| *count = count.saturating_add(1))
                    .or_insert(1);
            }
            if let Some(layout) = &handle.layout {
                payloads
                    .by_layout
                    .entry(layout.as_str().to_owned())
                    .and_modify(|count| *count = count.saturating_add(1))
                    .or_insert(1);
            }
        }
        daedalus_ffi_core::WireValue::List(items) => {
            for item in items {
                accumulate_payload_value(item, payloads);
            }
        }
        daedalus_ffi_core::WireValue::Record(fields) => {
            for item in fields.values() {
                accumulate_payload_value(item, payloads);
            }
        }
        daedalus_ffi_core::WireValue::Enum(value) => {
            if let Some(item) = &value.value {
                accumulate_payload_value(item, payloads);
            }
        }
        _ => {}
    }
}

fn is_cow_payload_handle(handle: &daedalus_ffi_core::WirePayloadHandle) -> bool {
    handle
        .metadata
        .get("ownership_mode")
        .and_then(serde_json::Value::as_str)
        == Some("cow")
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
            self.record_worker_telemetry(FfiWorkerTelemetry {
                worker_id: self.worker_id(),
                health_checks: 1,
                last_health: Some(format_runner_health(RunnerHealth::Degraded).to_owned()),
                ..Default::default()
            });
            return RunnerHealth::Degraded;
        };
        let health = match slot.as_mut() {
            None => RunnerHealth::Starting,
            Some(process) => match process.child.try_wait() {
                Ok(Some(_)) => RunnerHealth::Stopped,
                Ok(None) => RunnerHealth::Ready,
                Err(_) => RunnerHealth::Degraded,
            },
        };
        self.record_worker_telemetry(FfiWorkerTelemetry {
            worker_id: self.worker_id(),
            health_checks: 1,
            last_health: Some(format_runner_health(health).to_owned()),
            ..Default::default()
        });
        health
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
        let request_message = WorkerMessage::new(
            WorkerMessagePayload::Invoke(request),
            correlation_id.clone(),
        );
        let request_bytes = estimate_json_bytes(&request_message);
        if let WorkerMessagePayload::Invoke(request) = &request_message.payload {
            self.record_request_payload_telemetry(request);
        }
        let encode_started = Instant::now();
        Self::write_message(process, &request_message)?;
        self.record_worker_telemetry(FfiWorkerTelemetry {
            worker_id: self.worker_id(),
            request_bytes,
            encode_duration: encode_started.elapsed(),
            ..Default::default()
        });

        let mut events = Vec::<InvokeEvent>::new();
        loop {
            let decode_started = Instant::now();
            let message = match self.read_message(process, self.request_timeout) {
                Ok(message) => message,
                Err(err @ RunnerPoolError::RequestTimedOut { .. }) => {
                    self.record_worker_telemetry(FfiWorkerTelemetry {
                        worker_id: self.worker_id(),
                        decode_duration: decode_started.elapsed(),
                        timeout_failures: 1,
                        ..Default::default()
                    });
                    Self::stop_locked(&mut slot);
                    return Err(err);
                }
                Err(err) => {
                    self.record_worker_telemetry(FfiWorkerTelemetry {
                        worker_id: self.worker_id(),
                        decode_duration: decode_started.elapsed(),
                        malformed_responses: 1,
                        ..Default::default()
                    });
                    return Err(err);
                }
            };
            let decode_duration = decode_started.elapsed();
            let response_bytes = estimate_json_bytes(&message);
            match message.payload {
                WorkerMessagePayload::Event(event) => {
                    events.push(event);
                    self.record_worker_telemetry(FfiWorkerTelemetry {
                        worker_id: self.worker_id(),
                        response_bytes,
                        decode_duration,
                        raw_io_events: 1,
                        ..Default::default()
                    });
                }
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
                    self.record_worker_telemetry(FfiWorkerTelemetry {
                        worker_id: self.worker_id(),
                        response_bytes,
                        decode_duration,
                        ..Default::default()
                    });
                    return Ok(response);
                }
                WorkerMessagePayload::Error(error) => {
                    self.record_worker_telemetry(FfiWorkerTelemetry {
                        worker_id: self.worker_id(),
                        response_bytes,
                        decode_duration,
                        typed_errors: 1,
                        ..Default::default()
                    });
                    return Err(worker_error(error));
                }
                other => {
                    self.record_worker_telemetry(FfiWorkerTelemetry {
                        worker_id: self.worker_id(),
                        response_bytes,
                        decode_duration,
                        malformed_responses: 1,
                        ..Default::default()
                    });
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
        self.record_worker_telemetry(FfiWorkerTelemetry {
            worker_id: self.worker_id(),
            shutdowns: 1,
            last_health: Some(format_runner_health(RunnerHealth::Stopped).to_owned()),
            ..Default::default()
        });
        Ok(())
    }
}

fn format_runner_health(health: RunnerHealth) -> &'static str {
    match health {
        RunnerHealth::Ready => "ready",
        RunnerHealth::Starting => "starting",
        RunnerHealth::Degraded => "degraded",
        RunnerHealth::Stopped => "stopped",
    }
}

fn worker_protocol_error(err: WorkerProtocolError) -> RunnerPoolError {
    RunnerPoolError::Runner(format!("worker protocol error: {err}"))
}

fn worker_error(err: WorkerError) -> RunnerPoolError {
    RunnerPoolError::Runner(format!("worker error {}: {}", err.code, err.message))
}

fn estimate_json_bytes<T: serde::Serialize>(value: &T) -> u64 {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len() as u64)
        .unwrap_or(0)
}

impl StderrDrain {
    fn spawn(mut stderr: ChildStderr, limit: usize) -> Self {
        let capture = Arc::new(Mutex::new(StderrCapture::default()));
        let thread_capture = Arc::clone(&capture);
        let handle = thread::spawn(move || {
            let mut buffer = [0_u8; 4096];
            loop {
                match stderr.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(read) => {
                        let mut capture = thread_capture
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        let remaining = limit.saturating_sub(capture.bytes.len());
                        if remaining > 0 {
                            let keep = remaining.min(read);
                            capture.bytes.extend_from_slice(&buffer[..keep]);
                            if keep < read {
                                capture.truncated = true;
                            }
                        } else if read > 0 {
                            capture.truncated = true;
                        }
                    }
                    Err(err) => {
                        let mut capture = thread_capture
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        capture.read_error = Some(err.to_string());
                        break;
                    }
                }
            }
        });
        Self {
            capture,
            handle: Some(handle),
        }
    }

    fn snapshot(&self) -> String {
        let capture = self
            .capture
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut text = String::from_utf8_lossy(&capture.bytes).into_owned();
        if capture.truncated {
            text.push_str("...");
        }
        if let Some(error) = &capture.read_error {
            if !text.is_empty() {
                text.push_str("; ");
            }
            text.push_str("failed to read stderr: ");
            text.push_str(error);
        }
        text
    }

    fn finish(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Default for StderrDrain {
    fn default() -> Self {
        Self {
            capture: Arc::new(Mutex::new(StderrCapture::default())),
            handle: None,
        }
    }
}

impl Drop for PersistentWorkerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.stderr.finish();
    }
}

#[cfg(test)]
mod tests;
