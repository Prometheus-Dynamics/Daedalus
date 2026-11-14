//! The unix socket transport.

#![cfg(unix)]

use std::boxed::Box;
use std::io;
use std::net::Shutdown;
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::time::Duration;

use super::Acceptor;
use super::session::Channel;

/// Accepts connections on a unix socket.
pub struct UnixAcceptor {
    listener: UnixListener,
    path: Option<PathBuf>,
    /// The device and inode of the socket file this bound. Only that file is removed on close:
    /// a file that replaced it at the same path belongs to someone else.
    file: Option<(u64, u64)>,
    write_timeout: Option<Duration>,
    /// The listener is nonblocking, so the accept loop waits on its descriptor.
    #[cfg(target_os = "linux")]
    waits: bool,
}

impl UnixAcceptor {
    /// Binds `path`. A socket file nothing listens on is left over from a host that died, and is
    /// replaced. One that answers belongs to a live host, and the bind is refused.
    pub fn bind(path: &Path, write_timeout: Option<Duration>) -> io::Result<UnixAcceptor> {
        if path.exists() {
            if UnixStream::connect(path).is_ok() {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    std::format!("{} is in use by a live host", path.display()),
                ));
            }
            std::fs::remove_file(path)?;
        }
        let listener = UnixListener::bind(path)?;
        let file = file_id(path);
        let mut acceptor = UnixAcceptor::adopt(listener, write_timeout);
        acceptor.path = Some(path.to_path_buf());
        acceptor.file = file;
        Ok(acceptor)
    }

    /// Takes over a socket that is already listening, as socket activation hands one over. Its
    /// file is the activator's, so it is not removed when this closes.
    pub fn adopt(listener: UnixListener, write_timeout: Option<Duration>) -> UnixAcceptor {
        // One that can't be made nonblocking is still served, woken by connecting to it.
        #[cfg(target_os = "linux")]
        let waits = listener.set_nonblocking(true).is_ok();
        UnixAcceptor {
            listener,
            path: None,
            file: None,
            write_timeout,
            #[cfg(target_os = "linux")]
            waits,
        }
    }

    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }
}

/// The device and inode of the file at `path`, without following a link.
fn file_id(path: &Path) -> Option<(u64, u64)> {
    std::fs::symlink_metadata(path)
        .ok()
        .map(|meta| (meta.dev(), meta.ino()))
}

impl Acceptor for UnixAcceptor {
    fn accept(&mut self) -> io::Result<Channel> {
        let (stream, _) = self.listener.accept()?;
        // A connection is served blocking, whatever the listener is.
        stream.set_nonblocking(false)?;
        stream.set_write_timeout(self.write_timeout)?;
        let (reader, closer) = (stream.try_clone()?, stream.try_clone()?);
        Ok(Channel {
            reader: Box::new(reader),
            writer: Box::new(stream),
            close: Box::new(move || {
                let _ = closer.shutdown(Shutdown::Both);
            }),
        })
    }

    fn waker(&self) -> Box<dyn Fn() + Send + Sync> {
        match self.listener.local_addr() {
            Ok(addr) => Box::new(move || {
                let _ = UnixStream::connect_addr(&addr);
            }),
            Err(_) => Box::new(|| {}),
        }
    }

    #[cfg(target_os = "linux")]
    fn readable(&self) -> Option<std::os::fd::BorrowedFd<'_>> {
        self.waits.then(|| std::os::fd::AsFd::as_fd(&self.listener))
    }

    fn describe(&self) -> std::string::String {
        match self.listener.local_addr() {
            Ok(addr) => match addr.as_pathname() {
                Some(path) => std::format!("unix:{}", path.display()),
                None => "unix:(unnamed)".into(),
            },
            Err(_) => "unix:(unknown)".into(),
        }
    }
}

impl Drop for UnixAcceptor {
    fn drop(&mut self) {
        if let Some(path) = &self.path
            && self.file.is_some()
            && file_id(path) == self.file
        {
            let _ = std::fs::remove_file(path);
        }
    }
}
