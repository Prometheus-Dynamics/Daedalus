//! A host frame type for the frame tests: R8 or NV12 in host memory or (Linux) in a memfd
//! standing in for a dma-buf, with v3 metadata, counted drops and counted CPU accesses.

#![allow(dead_code)] // Not every test binary uses every helper.

use std::ffi::c_void;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

use daedalus::macros::plugin;
use daedalus::transport::{
    FrameColorimetry, FrameFormatKind, FrameInterface, FramePlane, FrameResidency, FrameSource,
    Payload, Residency, fourcc, frame_meta,
};
use daedalus::type_key;

pub const COUNTED_KEY: &str = "test:frames:counted";

enum Backing {
    Heap(Vec<Vec<u8>>),
    /// One memfd holding every plane back to back, mapped read-only.
    #[cfg(target_os = "linux")]
    Memfd {
        fd: i32,
        map: *const u8,
        len: usize,
        offsets: Vec<usize>,
        lens: Vec<usize>,
    },
}

/// A host frame whose drop is counted (exactly once is the contract under test).
#[type_key(COUNTED_KEY)]
pub struct Counted {
    width: u32,
    height: u32,
    format: u32,
    sequence: u64,
    backing: Backing,
    drops: Arc<AtomicUsize>,
    begun: AtomicUsize,
    ended: AtomicUsize,
}

// Safety: the memfd mapping is read-only and lives as long as the frame.
unsafe impl Send for Counted {}
// Safety: see above.
unsafe impl Sync for Counted {}

impl Counted {
    pub const EXPOSURE_NS: u64 = 16_666_667;

    fn new(
        width: u32,
        height: u32,
        format: &[u8; 4],
        sequence: u64,
        backing: Backing,
    ) -> (Arc<Self>, Arc<AtomicUsize>) {
        let drops = Arc::new(AtomicUsize::new(0));
        let frame = Arc::new(Self {
            width,
            height,
            format: fourcc(format),
            sequence,
            backing,
            drops: drops.clone(),
            begun: AtomicUsize::new(0),
            ended: AtomicUsize::new(0),
        });
        (frame, drops)
    }

    pub fn r8(width: u32, height: u32, pixels: Vec<u8>) -> (Arc<Self>, Arc<AtomicUsize>) {
        assert_eq!(pixels.len(), (width * height) as usize);
        Self::new(width, height, b"R8  ", 1, Backing::Heap(vec![pixels]))
    }

    /// NV12 with a luma ramp and grey chroma.
    pub fn nv12(width: u32, height: u32, sequence: u64) -> (Arc<Self>, Arc<AtomicUsize>) {
        let planes = nv12_planes(width, height);
        Self::new(width, height, b"NV12", sequence, Backing::Heap(planes))
    }

    /// [`Self::nv12`] in a memfd, planes at offsets of the one fd (as a dma-buf would be).
    #[cfg(target_os = "linux")]
    pub fn memfd_nv12(width: u32, height: u32, sequence: u64) -> (Arc<Self>, Arc<AtomicUsize>) {
        let planes = nv12_planes(width, height);
        let lens: Vec<usize> = planes.iter().map(Vec::len).collect();
        let offsets = vec![0, lens[0]];
        let bytes: Vec<u8> = planes.concat();
        // Safety: plain syscalls on a fresh fd; every result is checked.
        unsafe {
            let fd = libc::memfd_create(c"daedalus-frame".as_ptr(), libc::MFD_CLOEXEC);
            assert!(fd >= 0, "memfd_create");
            let written = libc::write(fd, bytes.as_ptr().cast(), bytes.len());
            assert_eq!(written, bytes.len() as isize);
            let map = libc::mmap(
                std::ptr::null_mut(),
                bytes.len(),
                libc::PROT_READ,
                libc::MAP_SHARED,
                fd,
                0,
            );
            assert_ne!(map, libc::MAP_FAILED);
            let backing = Backing::Memfd {
                fd,
                map: map.cast::<u8>().cast_const(),
                len: bytes.len(),
                offsets,
                lens,
            };
            Self::new(width, height, b"NV12", sequence, backing)
        }
    }

    /// The payload a camera stack would feed (the `Arc` shared, nothing copied).
    pub fn payload(self: &Arc<Self>) -> Payload {
        let residency = match self.residency() {
            FrameResidency::Cpu => Residency::Cpu,
            _ => Residency::External,
        };
        Payload::shared_with(COUNTED_KEY, self.clone(), residency, None, None)
    }

    /// The data pointer foreign handles of this frame carry.
    pub fn data_ptr(self: &Arc<Self>) -> *const c_void {
        Arc::as_ptr(self).cast()
    }

    pub fn luma_ptr(&self) -> *const u8 {
        self.bytes(0).expect("plane 0").as_ptr()
    }

    /// CPU accesses begun and ended so far.
    pub fn accesses(&self) -> (usize, usize) {
        (self.begun.load(SeqCst), self.ended.load(SeqCst))
    }

    #[cfg(target_os = "linux")]
    pub fn fd(&self) -> i32 {
        match &self.backing {
            Backing::Memfd { fd, .. } => *fd,
            Backing::Heap(_) => -1,
        }
    }

    #[cfg(target_os = "linux")]
    pub fn map_ptr(&self) -> *const u8 {
        match &self.backing {
            Backing::Memfd { map, .. } => *map,
            Backing::Heap(_) => std::ptr::null(),
        }
    }

    /// Whether the memfd is still open (consumers borrowed it, nobody closed it).
    #[cfg(target_os = "linux")]
    pub fn fd_is_open(&self) -> bool {
        // Safety: `F_GETFD` only queries the descriptor.
        unsafe { libc::fcntl(self.fd(), libc::F_GETFD) != -1 }
    }

    fn bytes(&self, index: u32) -> Option<&[u8]> {
        match &self.backing {
            Backing::Heap(planes) => planes.get(index as usize).map(Vec::as_slice),
            #[cfg(target_os = "linux")]
            Backing::Memfd {
                map, offsets, lens, ..
            } => {
                let (offset, len) = (*offsets.get(index as usize)?, lens[index as usize]);
                // Safety: the mapping covers every plane and lives as long as `self`.
                Some(unsafe { std::slice::from_raw_parts(map.add(offset), len) })
            }
        }
    }
}

fn nv12_planes(width: u32, height: u32) -> Vec<Vec<u8>> {
    let luma = (0..width * height).map(|i| i as u8).collect();
    let chroma = vec![128; (width * height / 2) as usize];
    vec![luma, chroma]
}

impl Drop for Counted {
    fn drop(&mut self) {
        #[cfg(target_os = "linux")]
        if let Backing::Memfd { fd, map, len, .. } = self.backing {
            // Safety: our own mapping and fd, released once.
            unsafe {
                libc::munmap(map.cast_mut().cast(), len);
                libc::close(fd);
            }
        }
        self.drops.fetch_add(1, SeqCst);
    }
}

impl FrameSource for Counted {
    fn width(&self) -> u32 {
        self.width
    }
    fn height(&self) -> u32 {
        self.height
    }
    fn format(&self) -> u32 {
        self.format
    }
    fn format_kind(&self) -> FrameFormatKind {
        FrameFormatKind::Pixel
    }
    fn sequence(&self) -> u64 {
        self.sequence
    }
    fn timestamp_ns(&self) -> u64 {
        1_000 * self.sequence
    }
    fn residency(&self) -> FrameResidency {
        match self.backing {
            Backing::Heap(_) => FrameResidency::Cpu,
            #[cfg(target_os = "linux")]
            Backing::Memfd { .. } => FrameResidency::External,
        }
    }
    fn plane_count(&self) -> u32 {
        match &self.backing {
            Backing::Heap(planes) => planes.len() as u32,
            #[cfg(target_os = "linux")]
            Backing::Memfd { lens, .. } => lens.len() as u32,
        }
    }
    fn plane(&self, index: u32) -> Option<FramePlane> {
        let stride = u64::from(self.width);
        match &self.backing {
            Backing::Heap(_) => Some(FramePlane::cpu(self.bytes(index)?, stride)),
            #[cfg(target_os = "linux")]
            Backing::Memfd {
                fd, offsets, lens, ..
            } => {
                let (offset, len) = (*offsets.get(index as usize)?, lens[index as usize]);
                Some(FramePlane::dmabuf(*fd, offset as u64, stride, len as u64))
            }
        }
    }
    fn plane_data(&self, index: u32) -> Option<&[u8]> {
        let bytes = self.bytes(index)?;
        self.begun.fetch_add(1, SeqCst);
        Some(bytes)
    }
    fn end_cpu_access(&self, _index: u32) {
        self.ended.fetch_add(1, SeqCst);
    }
    fn colorimetry(&self) -> FrameColorimetry {
        if self.format == fourcc(b"R8  ") {
            FrameColorimetry::SYCC
        } else {
            FrameColorimetry::SMPTE170M
        }
    }
    fn metadata_u64(&self, key: u32) -> Option<u64> {
        (key == frame_meta::EXPOSURE_NS).then_some(Self::EXPOSURE_NS)
    }
    fn metadata_bytes(&self, key: u32) -> Option<&[u8]> {
        (key == frame_meta::SENSOR_ID).then_some(b"imx477".as_slice())
    }
}

/// The host's frame integration: registers [`Counted`] and its `daedalus:frame` provider.
#[plugin(
    id = "test.frames.host",
    types(Counted),
    foreign_providers(Counted => FrameInterface)
)]
pub struct FramesHost;
