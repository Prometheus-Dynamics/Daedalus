//! One worker process: its child, its pipes, and the threads that read them. Starting, asking,
//! cancelling, killing and shutting down all happen here.

use std::collections::VecDeque;
use std::io::{ErrorKind, Read};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{RecvTimeoutError, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use daedalus_wire::codec::Writer;
use daedalus_wire::{PacketError, write_packet};

use crate::ProcessError;
use crate::reader::{self, Packets};
use crate::spec::WorkerSpec;
use crate::telemetry::{Count, Telemetry};

/// The stderr tail kept for a fault report.
pub(crate) const STDERR_CAP: usize = 64 * 1024;

/// The last bytes a worker wrote to stderr.
#[derive(Debug, Default)]
pub(crate) struct Ring {
    bytes: VecDeque<u8>,
}

impl Ring {
    fn push(&mut self, chunk: &[u8]) {
        let keep = &chunk[chunk.len().saturating_sub(STDERR_CAP)..];
        while self.bytes.len() + keep.len() > STDERR_CAP {
            self.bytes.pop_front();
        }
        self.bytes.extend(keep.iter().copied());
    }

    /// The last `max` bytes, as text.
    pub(crate) fn tail(&self, max: usize) -> String {
        let start = self.bytes.len().saturating_sub(max);
        let raw: Vec<u8> = self.bytes.iter().skip(start).copied().collect();
        String::from_utf8_lossy(&raw).into_owned()
    }
}

/// What one receive returned.
pub(crate) enum Got {
    Packet(Vec<u8>),
    /// The channel closed or broke: the worker is gone or sent something unreadable.
    Broken(PacketError),
    /// Nothing arrived before the timeout.
    Timeout,
}

pub(crate) struct Worker {
    child: Option<Child>,
    pid: u32,
    stdin: Option<ChildStdin>,
    packets: Option<Packets>,
    recycle: SyncSender<Vec<u8>>,
    stderr: Arc<Mutex<Ring>>,
    threads: Vec<JoinHandle<()>>,
    cap: u32,
    grace: Duration,
}

impl Worker {
    pub(crate) fn spawn(
        spec: &WorkerSpec,
        telemetry: &Arc<Telemetry>,
    ) -> Result<Worker, ProcessError> {
        let mut command = Command::new(&spec.program);
        command
            .args(&spec.args)
            .envs(spec.env.iter().map(|(key, value)| (key, value)))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(cwd) = &spec.cwd {
            command.current_dir(cwd);
        }
        if let Some(launch) = spec.launch {
            launch(&mut command);
        }
        let mut child = command.spawn().map_err(|error| ProcessError::Spawn {
            program: spec.program.clone(),
            message: error.to_string(),
        })?;
        telemetry.bump(Count::Spawn);
        let pid = child.id();
        let stdin = child.stdin.take().ok_or_else(|| ProcessError::Spawn {
            program: spec.program.clone(),
            message: "no stdin pipe".into(),
        })?;
        let stdout = child.stdout.take().ok_or_else(|| ProcessError::Spawn {
            program: spec.program.clone(),
            message: "no stdout pipe".into(),
        })?;
        let stderr_pipe = child.stderr.take().ok_or_else(|| ProcessError::Spawn {
            program: spec.program.clone(),
            message: "no stderr pipe".into(),
        })?;
        let (packets, recycle, reader_thread) = reader::spawn(stdout, spec.packet_cap);
        let ring = Arc::new(Mutex::new(Ring::default()));
        let drain_thread = drain(stderr_pipe, ring.clone(), telemetry.clone());
        Ok(Worker {
            child: Some(child),
            pid,
            stdin: Some(stdin),
            packets: Some(packets),
            recycle,
            stderr: ring,
            threads: vec![reader_thread, drain_thread],
            cap: spec.packet_cap,
            grace: spec.grace,
        })
    }

    pub(crate) fn pid(&self) -> u32 {
        self.pid
    }

    /// Writes one packet to the worker.
    pub(crate) fn send(&mut self, body: &[u8]) -> Result<(), PacketError> {
        match self.stdin.as_mut() {
            Some(stdin) => write_packet(stdin, body, self.cap),
            None => Err(PacketError::Closed),
        }
    }

    /// Waits for the next packet, up to `timeout` (or forever).
    pub(crate) fn receive(&self, timeout: Option<Duration>) -> Got {
        let Some(packets) = self.packets.as_ref() else {
            return Got::Broken(PacketError::Closed);
        };
        let result = match timeout {
            None => packets.recv().map_err(|_| RecvTimeoutError::Disconnected),
            Some(timeout) => packets.recv_timeout(timeout),
        };
        match result {
            Ok(Ok(buf)) => Got::Packet(buf),
            Ok(Err(error)) => Got::Broken(error),
            Err(RecvTimeoutError::Timeout) => Got::Timeout,
            Err(RecvTimeoutError::Disconnected) => Got::Broken(PacketError::Closed),
        }
    }

    /// Hands a packet's buffer back to the reader, so the next packet reuses it.
    pub(crate) fn give_back(&self, buf: Vec<u8>) {
        let _ = self.recycle.try_send(buf);
    }

    /// The last `max` bytes the worker wrote to stderr, for a fault report.
    pub(crate) fn stderr_tail(&self, max: usize) -> String {
        self.stderr
            .lock()
            .map(|ring| ring.tail(max))
            .unwrap_or_default()
    }

    /// Asks the worker to exit, waits the grace period, then kills it.
    pub(crate) fn shutdown(&mut self) {
        let mut writer = Writer::new();
        writer.u8(daedalus_wire::kind::SHUTDOWN);
        let _ = self.send(&writer.buf);
        self.stdin = None;
        let deadline = Instant::now() + self.grace;
        if let Some(child) = self.child.as_mut() {
            while Instant::now() < deadline {
                if matches!(child.try_wait(), Ok(Some(_))) {
                    break;
                }
                thread::sleep(Duration::from_millis(2));
            }
        }
        self.kill();
    }

    /// Kills the child this worker started, and waits for it and its threads to end.
    pub(crate) fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        self.stdin = None;
        // Dropping the receiver lets a reader blocked on a full channel end.
        self.packets = None;
        for handle in self.threads.drain(..) {
            let _ = handle.join();
        }
    }

    /// The body of a cancel for one run.
    pub(crate) fn cancel_body(run: u64) -> Vec<u8> {
        let mut writer = Writer::new();
        writer.u8(daedalus_wire::kind::CANCEL);
        writer.u64(run);
        writer.buf
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if self.child.is_some() {
            self.shutdown();
        }
    }
}

fn drain(
    mut pipe: impl Read + Send + 'static,
    ring: Arc<Mutex<Ring>>,
    telemetry: Arc<Telemetry>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut chunk = [0u8; 4096];
        loop {
            match pipe.read(&mut chunk) {
                Ok(0) => break,
                Ok(count) => {
                    telemetry.bump(Count::Stderr(count as u64));
                    if let Ok(mut ring) = ring.lock() {
                        ring.push(&chunk[..count]);
                    }
                }
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(_) => break,
            }
        }
    })
}
