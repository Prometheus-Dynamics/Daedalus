//! The TCP transport, on the loopback interface only.

use std::boxed::Box;
use std::io;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::time::Duration;

use super::Acceptor;
use super::session::Channel;

/// Accepts connections on a loopback TCP port.
pub struct TcpAcceptor {
    listener: TcpListener,
    addr: SocketAddr,
    write_timeout: Option<Duration>,
    /// The listener is nonblocking, so the accept loop waits on its descriptor.
    #[cfg(target_os = "linux")]
    waits: bool,
}

impl TcpAcceptor {
    /// Binds `addr`, which must be a loopback address: nothing listens on the network.
    pub fn bind(addr: SocketAddr, write_timeout: Option<Duration>) -> io::Result<TcpAcceptor> {
        if !addr.ip().is_loopback() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                std::format!("{addr} is not a loopback address"),
            ));
        }
        let listener = TcpListener::bind(addr)?;
        let addr = listener.local_addr()?;
        #[cfg(target_os = "linux")]
        let waits = listener.set_nonblocking(true).is_ok();
        Ok(TcpAcceptor {
            listener,
            addr,
            write_timeout,
            #[cfg(target_os = "linux")]
            waits,
        })
    }

    /// The address bound, with the port the system chose when the request was port 0.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Acceptor for TcpAcceptor {
    fn accept(&mut self) -> io::Result<Channel> {
        let (stream, peer) = self.listener.accept()?;
        if !peer.ip().is_loopback() {
            // A loopback listener is only reached from this machine. Anything else is dropped.
            let _ = stream.shutdown(Shutdown::Both);
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                std::format!("{peer} is not a loopback peer"),
            ));
        }
        // A connection is served blocking, whatever the listener is.
        stream.set_nonblocking(false)?;
        stream.set_write_timeout(self.write_timeout)?;
        let _ = stream.set_nodelay(true);
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
        let addr = self.addr;
        Box::new(move || {
            let _ = TcpStream::connect(addr);
        })
    }

    #[cfg(target_os = "linux")]
    fn readable(&self) -> Option<std::os::fd::BorrowedFd<'_>> {
        self.waits.then(|| std::os::fd::AsFd::as_fd(&self.listener))
    }

    fn describe(&self) -> std::string::String {
        std::format!("tcp:{}", self.addr)
    }
}
