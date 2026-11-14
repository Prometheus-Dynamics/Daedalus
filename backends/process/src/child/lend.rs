//! Values by reference, for a worker that asked for them (Linux).
//!
//! A run's `bytes` and opaque inputs are written into the host's lent buffer, or, for a buffer by
//! descriptor such as a dma-buf, sent as that descriptor the first time and by its slot after.
//! The run goes with a note that names a value buffer for the outputs: a free one, or none when
//! cells still hold them all. An output value the worker wrote there becomes the output cell's
//! bytes with no copy (`adopt`).

use daedalus_abi::SharedBytes;
use daedalus_wire::shared::Place;

use super::Worker;
use crate::telemetry::Telemetry;

/// How many value buffers a worker that agreed values by reference is lent. A run's outputs go
/// in one; while cells hold earlier outputs the host names another, so this many runs' outputs
/// can be held downstream at once before one goes in its packet.
#[cfg(target_os = "linux")]
pub(crate) const VALUES: u32 = 4;

#[cfg(target_os = "linux")]
impl Worker {
    /// Whether this worker agreed values by reference.
    pub(crate) fn by_reference(&self) -> bool {
        self.lent
            .lender
            .as_ref()
            .is_some_and(daedalus_process_shm::Lender::by_reference)
    }

    /// Starts writing a run's values. Lends larger buffers first when the last run had a value
    /// that didn't fit.
    pub(crate) fn begin_run(&mut self, telemetry: &Telemetry) {
        let want = self.lent.want.get();
        if self
            .lent
            .lender
            .as_ref()
            .is_some_and(|lender| lender.size() < want)
        {
            self.lend(want, telemetry);
        }
        if let Some(lender) = self.lent.lender.as_mut() {
            lender.begin_run();
        }
    }

    /// Writes an input value into the host's buffer, and gives its place. None when it doesn't
    /// fit beside the run's other values: it then goes in the packet, and the next run lends
    /// buffers that hold it.
    pub(crate) fn place(&mut self, bytes: &[u8]) -> Option<Place> {
        let placed = self.lent.lender.as_mut()?.place(bytes);
        if placed.is_none() {
            // Room for the value, the run's other values and the packet after them.
            let need = bytes.len().saturating_mul(2);
            self.lent.want.set(self.lent.want.get().max(need));
        }
        placed
    }

    /// The place of a value in a buffer by descriptor.
    pub(crate) fn fd_place(
        &mut self,
        fd: std::os::fd::BorrowedFd<'_>,
        offset: u64,
        len: u64,
    ) -> Result<Place, String> {
        let lender = self
            .lent
            .lender
            .as_mut()
            .ok_or("the worker was lent no memory")?;
        lender
            .fd_place(fd, offset, len)
            .map_err(|error| error.to_string())
    }

    /// An output value at `place` in a value buffer, for a cell to hold with no copy.
    pub(crate) fn adopt(&self, place: &Place) -> Option<SharedBytes> {
        self.lent.lender.as_ref()?.adopt(place)
    }

    /// Sends a run with its note: in the frame after the note when it is small, in the host's
    /// buffer after its values when it is large.
    pub(super) fn send_run_by_reference(
        &mut self,
        body: &[u8],
        telemetry: &Telemetry,
    ) -> Result<(), daedalus_wire::PacketError> {
        use crate::telemetry::Count;
        use daedalus_wire::PacketError;
        use std::os::fd::AsFd;
        let Some(super::Input::Socket(socket)) = &self.input else {
            return Err(PacketError::Closed);
        };
        let Some(lender) = self.lent.lender.as_mut() else {
            return Err(PacketError::Closed);
        };
        let out = lender.free_out();
        if out.is_none() {
            telemetry.bump(Count::OutsFull);
        }
        let packet = if body.len() >= daedalus_process_shm::THRESHOLD {
            let placed = lender.place_packet(body);
            if placed.is_some() {
                telemetry.bump(Count::SharedIn);
            } else {
                let need = body.len().saturating_mul(2);
                self.lent.want.set(self.lent.want.get().max(need));
            }
            placed
        } else {
            None
        };
        if body.len() > self.cap as usize && packet.is_none() {
            return Err(PacketError::TooLarge {
                len: u32::try_from(body.len()).unwrap_or(u32::MAX),
                cap: self.cap,
            });
        }
        let sent = lender
            .send_run(socket.as_fd(), out, packet, body)
            .map_err(|error| PacketError::Io(std::io::Error::other(error.to_string())))?;
        if sent > 0 {
            telemetry.bump(Count::FdSends(sent as u64));
        }
        Ok(())
    }
}

#[cfg(not(target_os = "linux"))]
impl Worker {
    pub(crate) fn by_reference(&self) -> bool {
        false
    }

    pub(crate) fn begin_run(&mut self, _telemetry: &Telemetry) {}

    pub(crate) fn place(&mut self, _bytes: &[u8]) -> Option<Place> {
        None
    }

    pub(crate) fn adopt(&self, _place: &Place) -> Option<SharedBytes> {
        None
    }
}
