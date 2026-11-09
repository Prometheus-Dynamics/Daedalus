//! A plan sent in chunks and put back together, with a bad chunk, a repeat and a gap on the way.

#![cfg(all(feature = "blob", feature = "receive"))]

#[path = "shared/blob_fixture.rs"]
mod fixture;

use daedalus_cpu::blob::receive::{
    BlobReceiver, BlobSink, CHUNK_OVERHEAD, Progress, ReceiveError, encode_chunk,
};
use daedalus_cpu::blob::{Interpreter, encode};
use fixture::{LIBRARY, plan};

fn frames(blob: &[u8], payload: usize) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    let mut offset = 0;
    while offset < blob.len() {
        let mut out = [0u8; 64];
        let len = encode_chunk(blob, offset, payload, &mut out).unwrap();
        offset += len - CHUNK_OVERHEAD;
        frames.push(out[..len].to_vec());
    }
    frames
}

#[test]
fn a_blob_received_in_chunks_loads_and_runs() {
    let blob = encode(&plan()).unwrap();
    let frames = frames(&blob, 32);
    assert!(frames.len() > 3);
    let mut buffer = vec![0u8; blob.len() + 16];
    let mut rx = BlobReceiver::new(&mut buffer[..]);

    rx.push(&frames[0]).unwrap();
    let mut bad = frames[1].clone();
    bad[CHUNK_OVERHEAD - 2] ^= 0x40;
    assert!(matches!(rx.push(&bad), Err(ReceiveError::Checksum { .. })));
    assert_eq!(
        rx.push(&frames[2]),
        Err(ReceiveError::Gap { resend_from: 32 })
    );
    let mut last = Progress::Partial {
        received: 0,
        len: 0,
    };
    for (index, frame) in frames.iter().enumerate().skip(1) {
        last = rx.push(frame).unwrap();
        assert_eq!(rx.push(frame), Ok(last), "chunk {index} again");
    }
    let len = blob.len() as u32;
    assert_eq!(last, Progress::Complete { len });
    assert_eq!(
        rx.push(&frames[0]),
        Ok(Progress::Partial { received: 32, len })
    );

    let mut running = Box::new(Interpreter::<1024>::new());
    running.load(&LIBRARY, &buffer[..blob.len()]).unwrap();
    let (input, output) = (running.input("in").unwrap(), running.output("out").unwrap());
    running.push(input, 2_u32).unwrap();
    running.tick().unwrap();
    assert_eq!(running.pop::<u32>(output), Ok(Some(6)));
}

struct Flash {
    pages: Vec<[u8; 16]>,
    next: usize,
}

impl BlobSink for Flash {
    fn capacity(&self) -> usize {
        self.pages.len() * 16
    }

    fn write(&mut self, offset: usize, bytes: &[u8]) -> Result<(), ReceiveError> {
        if offset != self.next {
            return Err(ReceiveError::Sink);
        }
        for (index, &byte) in bytes.iter().enumerate() {
            let at = offset + index;
            self.pages[at / 16][at % 16] = byte;
        }
        self.next += bytes.len();
        Ok(())
    }
}

#[test]
fn a_flash_sink_gets_contiguous_writes_and_too_large_blobs_are_refused() {
    let blob = encode(&plan()).unwrap();
    let mut rx = BlobReceiver::new(Flash {
        pages: vec![[0; 16]; blob.len() / 16 + 1],
        next: 0,
    });
    for frame in frames(&blob, 5) {
        rx.push(&frame).unwrap();
    }
    let flash = rx.into_sink();
    let stored: Vec<u8> = flash
        .pages
        .iter()
        .flatten()
        .copied()
        .take(blob.len())
        .collect();
    assert_eq!(stored, blob);

    let mut small = [0u8; 8];
    let mut rx = BlobReceiver::new(&mut small[..]);
    assert_eq!(
        rx.push(&frames(&blob, 5)[0]),
        Err(ReceiveError::TooLarge {
            len: blob.len() as u32,
            capacity: 8
        })
    );
    assert_eq!(rx.push(b"nope"), Err(ReceiveError::Frame));
}
