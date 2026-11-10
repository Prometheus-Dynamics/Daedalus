//! One thread per worker reads its stdout into packets. Buffers go back to the reader through a
//! recycle channel, so a steady state reads into memory it already has.

use std::io::Read;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread::{self, JoinHandle};

use daedalus_wire::{PacketError, read_packet};

pub(crate) type Packets = Receiver<Result<Vec<u8>, PacketError>>;

/// Starts the reader. Returns the packets, the channel that takes buffers back, and the thread.
pub(crate) fn spawn<R: Read + Send + 'static>(
    mut stdout: R,
    cap: u32,
) -> (Packets, SyncSender<Vec<u8>>, JoinHandle<()>) {
    let (packets_tx, packets_rx) = sync_channel::<Result<Vec<u8>, PacketError>>(4);
    let (recycle_tx, recycle_rx) = sync_channel::<Vec<u8>>(8);
    let handle = thread::spawn(move || {
        loop {
            let mut buf = recycle_rx.try_recv().unwrap_or_default();
            match read_packet(&mut stdout, cap, &mut buf) {
                Ok(()) => {
                    if packets_tx.send(Ok(buf)).is_err() {
                        break;
                    }
                }
                Err(error) => {
                    let _ = packets_tx.send(Err(error));
                    break;
                }
            }
        }
    });
    (packets_rx, recycle_tx, handle)
}
