//! One worker process: its child, its stream, and the threads that read them. Starting, asking,
//! cancelling, killing and shutting down all happen here.
//!
//! On Linux a worker's stdin and stdout are one unix socket, so the host can lend it shared
//! memory (`daedalus-process-shm`): a worker that asks is lent two buffers, and a large run and
//! its outputs travel in them instead of on the stream. A worker that also wants values by
//! reference is lent value buffers too, and takes and gives `bytes` values as places (`lend`).
//! Elsewhere they are two pipes.

use std::collections::VecDeque;
use std::io::{ErrorKind, Read, Write};
#[cfg(not(target_os = "linux"))]
use std::process::ChildStdin;
use std::process::{Child, Command, Stdio};
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

mod lend;

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
    /// The worker's outputs, in the buffer the host lent it: the first `len` bytes. Linux only.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    InBuffer(usize),
    /// The channel closed or broke: the worker is gone or sent something unreadable.
    Broken(PacketError),
    /// Nothing arrived before the timeout.
    Timeout,
}

/// Where the host writes to a worker.
enum Input {
    #[cfg(not(target_os = "linux"))]
    Pipe(ChildStdin),
    #[cfg(target_os = "linux")]
    Socket(std::os::unix::net::UnixStream),
}

impl Write for Input {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            #[cfg(not(target_os = "linux"))]
            Input::Pipe(pipe) => pipe.write(buf),
            #[cfg(target_os = "linux")]
            Input::Socket(socket) => socket.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            #[cfg(not(target_os = "linux"))]
            Input::Pipe(pipe) => pipe.flush(),
            #[cfg(target_os = "linux")]
            Input::Socket(socket) => socket.flush(),
        }
    }
}

/// The memory lent to a worker, on Linux.
#[cfg(target_os = "linux")]
#[derive(Default)]
struct Lent {
    lender: Option<daedalus_process_shm::Lender>,
    generation: u32,
    /// The largest outputs the worker had to send on the stream because they did not fit: the
    /// next run lends buffers at least this big.
    want: std::cell::Cell<usize>,
    note: Writer,
    /// The features the worker asked for beyond shared memory: none, or values by reference,
    /// and of any type.
    wants: u32,
    /// The smallest packet or value that goes through the lent buffers, once lent.
    threshold: usize,
    /// The spec's threshold, if it names one.
    shared_from: Option<usize>,
}

pub(crate) struct Worker {
    child: Option<Child>,
    pid: u32,
    input: Option<Input>,
    #[cfg(target_os = "linux")]
    lent: Box<Lent>,
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
        // What the worker doesn't inherit is taken away first, so a variable the spec also sets
        // is set.
        if spec.env_clear {
            command.env_clear();
        }
        for key in &spec.env_remove {
            command.env_remove(key);
        }
        #[cfg(target_os = "linux")]
        let spawn_error = |error: std::io::Error| ProcessError::Spawn {
            program: spec.program.clone(),
            message: error.to_string(),
        };
        command
            .args(&spec.args)
            .envs(spec.env.iter().map(|(key, value)| (key, value)))
            .stderr(Stdio::piped());
        // One socket is the worker's stdin and its stdout: it reads and writes it as it would
        // two pipes, and descriptors can go to it.
        #[cfg(target_os = "linux")]
        let socket = {
            let (host, child) = std::os::unix::net::UnixStream::pair().map_err(spawn_error)?;
            let child_out = child.try_clone().map_err(spawn_error)?;
            command
                .stdin(Stdio::from(std::os::fd::OwnedFd::from(child)))
                .stdout(Stdio::from(std::os::fd::OwnedFd::from(child_out)));
            host
        };
        #[cfg(not(target_os = "linux"))]
        command.stdin(Stdio::piped()).stdout(Stdio::piped());
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
        // The worker's ends of the socket close here, so the host sees the stream end when the
        // worker exits.
        drop(command);
        let pid = child.id();
        #[cfg(target_os = "linux")]
        let (input, stdout) = {
            let reader = socket.try_clone().map_err(spawn_error)?;
            (Input::Socket(socket), reader)
        };
        #[cfg(not(target_os = "linux"))]
        let (input, stdout) = {
            let stdin = child.stdin.take().ok_or_else(|| ProcessError::Spawn {
                program: spec.program.clone(),
                message: "no stdin pipe".into(),
            })?;
            let stdout = child.stdout.take().ok_or_else(|| ProcessError::Spawn {
                program: spec.program.clone(),
                message: "no stdout pipe".into(),
            })?;
            (Input::Pipe(stdin), stdout)
        };
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
            input: Some(input),
            #[cfg(target_os = "linux")]
            lent: Box::new(Lent {
                shared_from: spec.shared_from,
                ..Lent::default()
            }),
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
        match self.input.as_mut() {
            Some(input) => write_packet(input, body, self.cap),
            None => Err(PacketError::Closed),
        }
    }

    /// Writes a run to the worker: into the buffer lent to it when the run is large and the
    /// worker was lent memory, on the stream otherwise. The host sends a run only once the last
    /// one has answered, so the worker is never reading the buffer when it is written.
    pub(crate) fn send_run(
        &mut self,
        body: &[u8],
        telemetry: &Telemetry,
    ) -> Result<(), PacketError> {
        #[cfg(target_os = "linux")]
        if self.by_reference() {
            return self.send_run_by_reference(body, telemetry);
        }
        #[cfg(target_os = "linux")]
        if body.len() >= self.lent.threshold && self.lent.lender.is_some() {
            let need = body.len().max(self.lent.want.get());
            if self
                .lent
                .lender
                .as_ref()
                .is_some_and(|lender| lender.size() < need)
            {
                self.lend(need, telemetry);
            }
            if let Some(lender) = self.lent.lender.as_mut()
                && let Some(room) = lender.to_worker().get_mut(..body.len())
            {
                room.copy_from_slice(body);
                self.lent.note.buf.clear();
                daedalus_process_shm::Lender::note(body[0], body.len(), &mut self.lent.note);
                telemetry.bump(Count::SharedIn);
                return match self.input.as_mut() {
                    Some(input) => write_packet(input, &self.lent.note.buf, self.cap),
                    None => Err(PacketError::Closed),
                };
            }
        }
        #[cfg(not(target_os = "linux"))]
        let _ = telemetry;
        self.send(body)
    }

    /// Lends the worker buffers of at least `size` bytes, in place of any it had. One that
    /// can't be lent leaves the worker on the plain path.
    #[cfg(target_os = "linux")]
    fn lend(&mut self, size: usize, telemetry: &Telemetry) {
        use std::os::fd::AsFd;
        let Some(Input::Socket(socket)) = &self.input else {
            return;
        };
        self.lent.generation = self.lent.generation.wrapping_add(1).max(1);
        use daedalus_wire::shared::BY_REFERENCE;
        let values = if self.lent.wants & BY_REFERENCE != 0 {
            lend::VALUES
        } else {
            0
        };
        let features = self.lent.wants & lend::FEATURES;
        self.lent.threshold =
            self.lent
                .shared_from
                .unwrap_or(if features & daedalus_wire::shared::ANY_TYPE != 0 {
                    daedalus_process_shm::ANY_TYPE_THRESHOLD
                } else {
                    daedalus_process_shm::THRESHOLD
                });
        let lent = daedalus_process_shm::Lender::with_values(
            self.lent.generation,
            size.next_power_of_two(),
            values,
        )
        .map(|lender| lender.with_threshold(self.lent.threshold))
        .and_then(|lender| lender.grant(socket.as_fd()).map(|()| lender))
        .and_then(|mut lender| {
            if lender.by_reference() {
                lender.agree(socket.as_fd(), features)?;
            }
            Ok(lender)
        });
        match lent {
            Ok(lender) => {
                telemetry.bump(Count::Grant);
                if lender.by_reference() {
                    telemetry.bump(Count::Agree);
                }
                self.lent.lender = Some(lender);
            }
            Err(_) => {
                telemetry.bump(Count::Plain);
                self.lent.lender = None;
            }
        }
    }

    /// The first exchange after the hello. The host pings; a worker that can map shared memory
    /// asks for it before it answers, and is then lent buffers or told why not. A worker that
    /// doesn't ask stays on the plain path, as every worker off Linux does.
    pub(crate) fn offer(&mut self, startup: Duration, telemetry: &Telemetry) -> Result<(), String> {
        #[cfg(target_os = "linux")]
        {
            use daedalus_wire::shared::{Shared, VERSION, kind as shared_kind};
            if !matches!(self.input, Some(Input::Socket(_))) {
                telemetry.bump(Count::Plain);
                return Ok(());
            }
            let mut ping = Writer::new();
            ping.u8(daedalus_wire::kind::PING);
            self.send(&ping.buf)
                .map_err(|error| format!("could not ping the worker: {error}"))?;
            let deadline = Instant::now() + startup;
            let mut asked = None;
            loop {
                let left = deadline.saturating_duration_since(Instant::now());
                match self.receive(Some(left)) {
                    Got::Packet(buf) => {
                        let kind = buf.first().copied();
                        if kind == Some(shared_kind::ASK) {
                            asked = match Shared::decode(&buf) {
                                Ok(Shared::Ask { version }) => Some(version),
                                _ => Some(0),
                            };
                        }
                        if kind == Some(shared_kind::WANT)
                            && let Ok(Shared::Want { features }) = Shared::decode(&buf)
                        {
                            self.lent.wants = features;
                        }
                        self.give_back(buf);
                        if kind == Some(daedalus_wire::kind::PONG) {
                            break;
                        }
                    }
                    Got::InBuffer(_) => {}
                    Got::Timeout => {
                        return Err(format!("no answer to the first ping within {startup:?}"));
                    }
                    Got::Broken(error) => {
                        return Err(format!("the worker closed after its hello: {error}"));
                    }
                }
            }
            match asked {
                None => telemetry.bump(Count::Plain),
                Some(VERSION) => {
                    self.lend(daedalus_process_shm::START_SIZE, telemetry);
                    if self.lent.lender.is_none() {
                        self.refuse("the host could not lend shared memory", telemetry);
                    }
                }
                Some(other) => self.refuse(
                    &format!("the worker speaks shared-memory version {other}, the host {VERSION}"),
                    telemetry,
                ),
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = startup;
            telemetry.bump(Count::Plain);
        }
        Ok(())
    }

    /// Tells a worker that asked for shared memory that it gets none, and why.
    #[cfg(target_os = "linux")]
    fn refuse(&mut self, reason: &str, telemetry: &Telemetry) {
        let mut out = Writer::new();
        let plain = daedalus_wire::shared::Shared::Plain {
            reason: reason.to_owned(),
        };
        if plain.encode(true, &mut out).is_ok() {
            let _ = self.send(&out.buf);
        }
        // `lend` counted the failed lending already; a version refusal is counted here.
        if self.lent.generation == 0 {
            telemetry.bump(Count::Plain);
        }
    }

    /// The first `len` bytes of the buffer the worker writes its outputs into.
    #[cfg(target_os = "linux")]
    pub(crate) fn worker_wrote(&self, len: usize) -> Option<&[u8]> {
        self.lent.lender.as_ref()?.worker_wrote(len)
    }

    #[cfg(not(target_os = "linux"))]
    pub(crate) fn worker_wrote(&self, _len: usize) -> Option<&[u8]> {
        None
    }

    /// Stops writing to the worker, which then reads the end of its stdin.
    fn close_input(&mut self) {
        #[cfg(target_os = "linux")]
        if let Some(Input::Socket(socket)) = &self.input {
            // The reader holds the same socket, so dropping this end alone would not end it.
            let _ = socket.shutdown(std::net::Shutdown::Write);
        }
        self.input = None;
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
            #[cfg(target_os = "linux")]
            Ok(Ok(buf)) if buf.first() == Some(&daedalus_wire::shared::kind::WORKER_IN_BUFFER) => {
                let note = daedalus_wire::shared::Shared::decode(&buf);
                self.give_back(buf);
                match note {
                    Ok(daedalus_wire::shared::Shared::InBuffer {
                        kind: daedalus_wire::kind::OUTPUTS,
                        len,
                        ..
                    }) => Got::InBuffer(len as usize),
                    _ => Got::Broken(PacketError::Io(std::io::Error::other(
                        "the worker named a buffer for something other than its outputs",
                    ))),
                }
            }
            Ok(Ok(buf)) => {
                // Outputs too large for the buffer lent came on the stream: the next run lends
                // buffers that hold them.
                #[cfg(target_os = "linux")]
                if self.lent.lender.is_some()
                    && buf.len() >= self.lent.threshold
                    && buf.first() == Some(&daedalus_wire::kind::OUTPUTS)
                {
                    self.lent.want.set(self.lent.want.get().max(buf.len()));
                }
                Got::Packet(buf)
            }
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

    /// Keeps a line the worker logged with its stderr, so a fault report has it.
    pub(crate) fn heard(&self, level: daedalus_wire::LogLevel, text: &str) {
        let line = format!("[{}] {text}\n", crate::log::name(level));
        if let Ok(mut ring) = self.stderr.lock() {
            ring.push(line.as_bytes());
        }
    }

    /// Asks the worker to exit, waits the grace period, then kills it.
    pub(crate) fn shutdown(&mut self) {
        let mut writer = Writer::new();
        writer.u8(daedalus_wire::kind::SHUTDOWN);
        let _ = self.send(&writer.buf);
        self.close_input();
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
        // The child is reaped: nothing maps the lent buffers but this host any more.
        self.close_input();
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
