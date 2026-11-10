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
        Ok(TcpAcceptor {
            listener,
            addr,
            write_timeout,
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

    fn describe(&self) -> std::string::String {
        std::format!("tcp:{}", self.addr)
    }
}
