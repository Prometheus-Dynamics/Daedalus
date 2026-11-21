//! E14's consumer in a second process: this program again, started as `dma-wait-peer`, given the
//! dma-buf over a Unix socket (`SCM_RIGHTS`, as a real consumer in another process gets one), and
//! asked for one copy at a time. It imports the buffer on a device of its own and copies it out
//! with no wait, then says what the copy saw.
//!
//! The protocol is lines on the socket: the parent sends `copy ROUNDS SEED` or `quit`; the peer
//! answers `ready` or `skip REASON` once, then `sentinel`, `gpu`, `other` or `error REASON` a copy.

use std::io::{BufRead, BufReader, ErrorKind, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::process::{Child, Command, ExitCode, Stdio};
use std::time::{Duration, Instant};

use daedalus_dma_sys::{DmaBuf, recv_fds, send_fds};
use daedalus_wgpu::{Options, RawPlacement, raw_copy_after};

use super::dmabuf::{BYTES, import_in_place, open_importing, sampled_correct};
use super::dmabuf_fence::saw_sentinel;

/// The section name the peer is started with. Not a section: `main` hands it here.
pub const PEER: &str = "dma-wait-peer";

/// How long the peer may take to start and open its device.
const START: Duration = Duration::from_secs(60);

/// What one copy saw.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Seen {
    /// Every sampled word still the sentinel: the copy ran before the write.
    Sentinel,
    /// The write's values: the copy ran after it.
    Gpu,
    /// Neither: a copy that overlapped the write.
    Other,
}

/// What the copy in `bytes` saw of a write of `rounds` and `seed`.
pub(super) fn seen(bytes: &[u8], rounds: u32, seed: u32) -> Seen {
    if saw_sentinel(bytes) {
        Seen::Sentinel
    } else if sampled_correct(bytes, rounds, seed) {
        Seen::Gpu
    } else {
        Seen::Other
    }
}

/// The parent's end: the peer process and the socket to it.
pub(super) struct Peer {
    child: Child,
    stream: UnixStream,
    reader: BufReader<UnixStream>,
    path: PathBuf,
}

impl Peer {
    /// Starts the peer on the adapter named `adapter` and hands it `buf`. `Err` with the reason
    /// when it can't start, or when it can't import the buffer in place.
    pub(super) fn spawn(adapter: &str, buf: &DmaBuf) -> Result<Peer, String> {
        let path = std::env::temp_dir().join(format!("dd-dma-peer-{}.sock", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path).map_err(|error| format!("the socket: {error}"))?;
        let exe = std::env::current_exe().map_err(|error| format!("this program: {error}"))?;
        let mut child = Command::new(exe)
            .arg(PEER)
            .arg(&path)
            .arg(adapter)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .spawn()
            .map_err(|error| format!("the peer doesn't start: {error}"))?;
        let stream = match accept(&listener, &mut child) {
            Ok(stream) => stream,
            Err(reason) => {
                stop(&mut child);
                let _ = std::fs::remove_file(&path);
                return Err(reason);
            }
        };
        let reader = match stream.try_clone() {
            Ok(clone) => BufReader::new(clone),
            Err(error) => {
                stop(&mut child);
                let _ = std::fs::remove_file(&path);
                return Err(format!("the socket doesn't duplicate: {error}"));
            }
        };
        let mut peer = Peer {
            child,
            stream,
            reader,
            path,
        };
        send_fds(&peer.stream, b"b", &[buf.as_fd()])
            .map_err(|error| format!("the dma-buf wasn't handed over: {error}"))?;
        match peer.reply(Some(START)) {
            Some(line) if line == "ready" => Ok(peer),
            Some(line) => Err(line.strip_prefix("skip ").unwrap_or(&line).to_string()),
            None => Err(format!("the peer wasn't ready in {} s", START.as_secs())),
        }
    }

    /// Asks the peer for one copy, made with no wait, of a write of `rounds` and `seed`.
    pub(super) fn copy(&mut self, rounds: u32, seed: u32) -> Result<(), String> {
        writeln!(self.stream, "copy {rounds} {seed}").map_err(|error| error.to_string())
    }

    /// What the last copy saw: `None` when no answer came within `timeout` (`None` waits).
    pub(super) fn seen(&mut self, timeout: Option<Duration>) -> Option<Result<Seen, String>> {
        let line = self.reply(timeout)?;
        Some(match line.as_str() {
            "sentinel" => Ok(Seen::Sentinel),
            "gpu" => Ok(Seen::Gpu),
            "other" => Ok(Seen::Other),
            other => Err(other.strip_prefix("error ").unwrap_or(other).to_string()),
        })
    }

    fn reply(&mut self, timeout: Option<Duration>) -> Option<String> {
        self.stream.set_read_timeout(timeout).ok()?;
        let mut line = String::new();
        match self.reader.read_line(&mut line) {
            Ok(0) => Some("error the peer closed the socket".to_string()),
            Ok(_) => Some(line.trim_end().to_string()),
            Err(error) if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                None
            }
            Err(error) => Some(format!("error the socket: {error}")),
        }
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        let _ = writeln!(self.stream, "quit");
        stop(&mut self.child);
        let _ = std::fs::remove_file(&self.path);
    }
}

fn accept(listener: &UnixListener, child: &mut Child) -> Result<UnixStream, String> {
    listener
        .set_nonblocking(true)
        .map_err(|error| error.to_string())?;
    let deadline = Instant::now() + START;
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                stream
                    .set_nonblocking(false)
                    .map_err(|error| error.to_string())?;
                return Ok(stream);
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => {}
            Err(error) => return Err(format!("the socket: {error}")),
        }
        if let Ok(Some(status)) = child.try_wait() {
            return Err(format!("the peer ended before it connected: {status}"));
        }
        if Instant::now() > deadline {
            return Err(format!("the peer didn't connect in {} s", START.as_secs()));
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Waits for the peer this program started to end, and stops it past ten seconds.
fn stop(child: &mut Child) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if !matches!(child.try_wait(), Ok(None)) {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let _ = child.kill();
    let _ = child.wait();
}

/// The peer: `dma-wait-peer SOCKET ADAPTER`.
pub fn serve(args: &[String]) -> ExitCode {
    let [path, adapter] = args else {
        return ExitCode::FAILURE;
    };
    let Ok(mut stream) = UnixStream::connect(path) else {
        return ExitCode::FAILURE;
    };
    let mut byte = [0u8; 1];
    let buf = match recv_fds(&stream, &mut byte) {
        Ok((1, fds)) if fds.len() == 1 => fds.into_iter().next().map(DmaBuf::from_fd),
        _ => None,
    };
    let Some(buf) = buf else {
        let _ = writeln!(stream, "skip the dma-buf didn't arrive");
        return ExitCode::FAILURE;
    };
    let options = Options {
        adapter: Some(adapter.clone()),
        ..Options::default()
    };
    let opened = open_importing(options).and_then(|(consumer, mut step)| {
        let source = import_in_place(&consumer, &mut step, &buf)?;
        let submitter = consumer
            .raw_submitter(2)
            .map_err(|error| error.to_string())?;
        let block = consumer
            .raw_block(BYTES, RawPlacement::HostMapped)
            .map_err(|error| error.to_string())?;
        Ok((consumer, source, submitter, block))
    });
    let (consumer, source, submitter, block) = match opened {
        Ok(opened) => opened,
        Err(reason) => {
            let _ = writeln!(stream, "skip {reason}");
            return ExitCode::FAILURE;
        }
    };
    let Ok(reader) = stream.try_clone() else {
        return ExitCode::FAILURE;
    };
    if writeln!(stream, "ready").is_err() {
        return ExitCode::FAILURE;
    }
    for line in BufReader::new(reader).lines() {
        let Ok(line) = line else { break };
        let mut words = line.split_whitespace();
        let (Some("copy"), Some(rounds), Some(seed)) = (words.next(), words.next(), words.next())
        else {
            break;
        };
        let (Ok(rounds), Ok(seed)) = (rounds.parse(), seed.parse()) else {
            break;
        };
        let copied = raw_copy_after(&submitter, &source, &block, BYTES, &[], || {});
        consumer.attach_after_submit();
        let mut bytes = vec![0u8; BYTES as usize];
        let answer = match copied
            .map_err(|error| error.to_string())
            .and_then(|()| block.read(0, &mut bytes).map_err(|error| error.to_string()))
        {
            Ok(()) => match seen(&bytes, rounds, seed) {
                Seen::Sentinel => "sentinel".to_string(),
                Seen::Gpu => "gpu".to_string(),
                Seen::Other => "other".to_string(),
            },
            Err(reason) => format!("error {reason}"),
        };
        if writeln!(stream, "{answer}").is_err() {
            break;
        }
    }
    ExitCode::SUCCESS
}
