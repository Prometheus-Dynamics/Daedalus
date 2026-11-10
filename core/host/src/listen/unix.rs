//! The unix socket transport.

#![cfg(unix)]

use std::boxed::Box;
use std::io;
use std::net::Shutdown;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::time::Duration;

use super::Acceptor;
use super::session::Channel;

/// Accepts connections on a unix socket.
pub struct UnixAcceptor {
    listener: UnixListener,
    path: Option<PathBuf>,
    write_timeout: Option<Duration>,
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
        Ok(UnixAcceptor {
            listener: UnixListener::bind(path)?,
            path: Some(path.to_path_buf()),
            write_timeout,
        })
    }

    /// Takes over a socket that is already listening, as socket activation hands one over. Its
    /// file is the activator's, so it is not removed when this closes.
    pub fn adopt(listener: UnixListener, write_timeout: Option<Duration>) -> UnixAcceptor {
        UnixAcceptor {
            listener,
            path: None,
            write_timeout,
        }
    }

    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }
}

impl Acceptor for UnixAcceptor {
    fn accept(&mut self) -> io::Result<Channel> {
        let (stream, _) = self.listener.accept()?;
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
        if let Some(path) = &self.path {
            let _ = std::fs::remove_file(path);
        }
    }
}
