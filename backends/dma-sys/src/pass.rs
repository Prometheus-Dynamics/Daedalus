//! Handing fds to another process over a Unix socket (`SCM_RIGHTS`): how a dma-buf or a sync_file
//! crosses a process boundary. The receiver gets its own fds to the same files.

use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;

use crate::Error;

/// The most fds one message carries.
pub const MAX_FDS: usize = 8;

/// Room for `MAX_FDS` fds of control data, aligned as the kernel's `cmsghdr` needs.
#[repr(C, align(8))]
struct Control([u8; 64]);

fn space(fds: usize) -> usize {
    let bytes = (fds * size_of::<RawFd>()) as u32;
    // SAFETY: only computes a size.
    unsafe { libc::CMSG_SPACE(bytes) as usize }
}

/// Sends `bytes` (at least one byte) with `fds` attached, as one message. At most [`MAX_FDS`].
pub fn send_fds(stream: &UnixStream, bytes: &[u8], fds: &[BorrowedFd<'_>]) -> Result<(), Error> {
    if bytes.is_empty() || fds.len() > MAX_FDS || space(MAX_FDS) > size_of::<Control>() {
        return Err(Error::Range);
    }
    let raw: Vec<RawFd> = fds.iter().map(AsRawFd::as_raw_fd).collect();
    let mut control = Control([0; 64]);
    let mut iov = libc::iovec {
        iov_base: bytes.as_ptr().cast_mut().cast(),
        iov_len: bytes.len(),
    };
    // SAFETY: a msghdr is plain data, all zeros is a valid empty one.
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    if !raw.is_empty() {
        let data = size_of_val(raw.as_slice());
        msg.msg_control = control.0.as_mut_ptr().cast();
        msg.msg_controllen = space(raw.len()) as _;
        // SAFETY: the control buffer is aligned and holds `space(raw.len())` bytes, so the first
        // header is in it and its data has room for `data` bytes.
        unsafe {
            let header = libc::CMSG_FIRSTHDR(&msg);
            (*header).cmsg_level = libc::SOL_SOCKET;
            (*header).cmsg_type = libc::SCM_RIGHTS;
            (*header).cmsg_len = libc::CMSG_LEN(data as u32) as _;
            std::ptr::copy_nonoverlapping(raw.as_ptr().cast::<u8>(), libc::CMSG_DATA(header), data);
        }
    }
    // SAFETY: the message points at `bytes` and `control`, both live for the call.
    let sent = unsafe { libc::sendmsg(stream.as_raw_fd(), &msg, libc::MSG_NOSIGNAL) };
    if sent < 0 {
        return Err(Error::last());
    }
    // A stream socket sends a message this short whole, or the fds didn't go with all of it.
    if sent as usize != bytes.len() {
        return Err(Error::Range);
    }
    Ok(())
}

/// Receives one message into `bytes`, with the fds attached to it, each close-on-exec. Returns the
/// bytes read (0 when the other end has closed) and the fds.
pub fn recv_fds(stream: &UnixStream, bytes: &mut [u8]) -> Result<(usize, Vec<OwnedFd>), Error> {
    let mut control = Control([0; 64]);
    let mut iov = libc::iovec {
        iov_base: bytes.as_mut_ptr().cast(),
        iov_len: bytes.len(),
    };
    // SAFETY: as in `send_fds`.
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.0.as_mut_ptr().cast();
    msg.msg_controllen = space(MAX_FDS) as _;
    // SAFETY: the message points at `bytes` and `control`, both live for the call.
    let got = unsafe { libc::recvmsg(stream.as_raw_fd(), &mut msg, libc::MSG_CMSG_CLOEXEC) };
    if got < 0 {
        return Err(Error::last());
    }
    let mut fds = Vec::new();
    // SAFETY: the kernel filled `control` up to `msg_controllen`; the CMSG macros walk only
    // inside it, and each SCM_RIGHTS fd is new and this process's to own.
    unsafe {
        let mut header = libc::CMSG_FIRSTHDR(&msg);
        while !header.is_null() {
            if (*header).cmsg_level == libc::SOL_SOCKET && (*header).cmsg_type == libc::SCM_RIGHTS {
                let data = libc::CMSG_DATA(header);
                let len = (*header).cmsg_len as usize - (data as usize - header as usize);
                for index in 0..len / size_of::<RawFd>() {
                    let raw = std::ptr::read_unaligned(data.cast::<RawFd>().add(index));
                    fds.push(OwnedFd::from_raw_fd(raw));
                }
            }
            header = libc::CMSG_NXTHDR(&msg, header);
        }
    }
    if msg.msg_flags & libc::MSG_CTRUNC != 0 {
        // More fds came than there was room for: the kernel closed the rest.
        return Err(Error::Range);
    }
    Ok((got as usize, fds))
}
