//! A capture device for a test: it streams a V4L2 camera and hands each filled buffer out as a
//! dma-buf, so a test can read the frame where the camera wrote it. Test-only. The raw requests
//! are in `daedalus-camera-fixture-sys`.

#![forbid(unsafe_code)]
#![cfg(target_os = "linux")]

use std::fs::{self, OpenOptions};
use std::io;
use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};

use daedalus_camera_fixture_sys as sys;

/// Buffers the fixture asks the driver for: enough that one can be read while the next fills.
const BUFFERS: u32 = 4;
const EBUSY: i32 = 16;

/// Why a device could not be streamed.
#[derive(Debug)]
pub enum Error {
    /// Another user holds the device (`EBUSY`): a service is capturing from it. Nothing was
    /// changed on the device.
    Busy { path: PathBuf, step: &'static str },
    /// The node is not a capture device that streams.
    NotCapture(PathBuf),
    /// Any other failure, with the request it came from.
    Io {
        path: PathBuf,
        step: &'static str,
        error: io::Error,
    },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Busy { path, step } => write!(f, "{} is busy ({step}: EBUSY)", path.display()),
            Error::NotCapture(path) => {
                write!(f, "{} is not a streaming capture node", path.display())
            }
            Error::Io { path, step, error } => write!(f, "{} ({step}): {error}", path.display()),
        }
    }
}

/// The video nodes in `/dev`, lowest number first. Their capabilities are checked on open.
pub fn capture_nodes() -> Vec<PathBuf> {
    let mut nodes: Vec<(u32, PathBuf)> = fs::read_dir("/dev")
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| {
            let path = entry.path();
            let number = path
                .file_name()?
                .to_str()?
                .strip_prefix("video")?
                .parse()
                .ok()?;
            Some((number, path))
        })
        .collect();
    nodes.sort_by_key(|(number, _)| *number);
    nodes.into_iter().map(|(_, path)| path).collect()
}

/// One filled buffer: which of the camera's buffers holds it, and how many bytes were written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Frame {
    pub index: u32,
    pub bytes: u32,
    pub sequence: u32,
}

/// The pixel format the device settled on.
pub use sys::Pix;

/// A streaming capture device. Dropping it stops the stream and frees the buffers.
pub struct Camera {
    path: PathBuf,
    file: fs::File,
    format: Pix,
    dmabufs: Vec<OwnedFd>,
}

impl Camera {
    /// Opens `path` and streams `fourcc` at `width` by `height`. The device may pick other
    /// dimensions; [`Camera::format`] says what it picked.
    pub fn open(path: &Path, fourcc: [u8; 4], width: u32, height: u32) -> Result<Camera, Error> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|error| io_error(path, "open", error))?;
        let (_, streaming) =
            sys::capture_caps(&file).map_err(|error| io_error(path, "querycap", error))?;
        if !streaming {
            return Err(Error::NotCapture(path.to_path_buf()));
        }
        let format = sys::set_format(&file, u32::from_le_bytes(fourcc), width, height)
            .map_err(|error| classify(path, "s_fmt", error))?;
        let count = sys::request_buffers(&file, BUFFERS)
            .map_err(|error| classify(path, "reqbufs", error))?;
        let mut camera = Camera {
            path: path.to_path_buf(),
            file,
            format,
            dmabufs: Vec::with_capacity(count as usize),
        };
        for index in 0..count {
            let fd = sys::export(&camera.file, index)
                .map_err(|error| classify(path, "expbuf", error))?;
            camera.dmabufs.push(fd);
            sys::queue(&camera.file, index).map_err(|error| classify(path, "qbuf", error))?;
        }
        sys::stream(&camera.file, true).map_err(|error| classify(path, "streamon", error))?;
        Ok(camera)
    }

    /// The format the device gave.
    pub fn format(&self) -> Pix {
        self.format
    }

    /// The node this camera was opened on.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The dma-buf of buffer `index`. A clone of it names the same memory.
    pub fn dmabuf(&self, index: u32) -> Option<&OwnedFd> {
        self.dmabufs.get(index as usize)
    }

    /// Waits for the next filled buffer. Until [`Camera::requeue`] gives it back, the camera
    /// does not write into it.
    pub fn next_frame(&mut self) -> io::Result<Frame> {
        let filled = sys::dequeue(&self.file)?;
        Ok(Frame {
            index: filled.index,
            bytes: filled.bytes,
            sequence: filled.sequence,
        })
    }

    /// Gives buffer `index` back to the camera.
    pub fn requeue(&self, index: u32) -> io::Result<()> {
        sys::queue(&self.file, index)
    }
}

impl Drop for Camera {
    fn drop(&mut self) {
        // Errors here leave nothing to undo: the device is closed when the file goes.
        let _ = sys::stream(&self.file, false);
        let _ = sys::release_buffers(&self.file);
    }
}

fn io_error(path: &Path, step: &'static str, error: io::Error) -> Error {
    Error::Io {
        path: path.to_path_buf(),
        step,
        error,
    }
}

/// `EBUSY` from any request is another user of the device, and anything else is an error.
fn classify(path: &Path, step: &'static str, error: io::Error) -> Error {
    if error.raw_os_error() == Some(EBUSY) {
        Error::Busy {
            path: path.to_path_buf(),
            step,
        }
    } else {
        io_error(path, step, error)
    }
}
