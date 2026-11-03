//! A plan blob sent in chunks through `encode_chunk` and assembled by `BlobReceiver`, with a
//! corrupted frame, a retransmission and a gap on the way, then loaded and run.

use super::Interpreter;
use super::receive::{
    BlobReceiver, BlobSink, CHUNK_OVERHEAD, Progress, ReceiveError, encode_chunk,
};
use super::tests::{LIBRARY, plan};

/// The frames of `blob`, `payload` bytes each.
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
    let blob = plan();
    let frames = frames(&blob, 7);
    assert!(frames.len() > 3);
    let mut buffer = [0u8; 128];
    let mut rx = BlobReceiver::new(&mut buffer[..]);

    rx.push(&frames[0]).unwrap();
    // A frame corrupted in transit is refused and changes nothing.
    let mut bad = frames[1].clone();
    bad[CHUNK_OVERHEAD - 2] ^= 0x40;
    assert!(matches!(rx.push(&bad), Err(ReceiveError::Checksum { .. })));
    // Skipping a chunk asks for it again.
    assert_eq!(
        rx.push(&frames[2]),
        Err(ReceiveError::Gap { resend_from: 7 })
    );
    let mut last = Progress::Partial {
        received: 0,
        len: 0,
    };
    for (i, frame) in frames.iter().enumerate().skip(1) {
        last = rx.push(frame).unwrap();
        // A retransmission of the chunk just taken is ignored.
        assert_eq!(rx.push(frame), Ok(last), "chunk {i} again");
    }
    let len = blob.len() as u32;
    assert_eq!(last, Progress::Complete { len });
    // The first chunk again while complete starts the blob over.
    assert_eq!(
        rx.push(&frames[0]),
        Ok(Progress::Partial { received: 7, len })
    );

    let mut interp = Box::new(Interpreter::<256>::new(&LIBRARY));
    interp.load(&buffer[..blob.len()]).unwrap();
    assert_eq!(interp.plan_hash(), Some(7));
    let (a, out) = (interp.input("a").unwrap(), interp.output("out").unwrap());
    interp.push(a, true).unwrap();
    interp.tick().unwrap();
    assert_eq!(interp.pop::<bool>(out), Ok(Some(true)));
}

/// A flash partition stand-in: page writes only, at increasing offsets.
struct Flash {
    pages: [[u8; 16]; 8],
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
        for (i, &byte) in bytes.iter().enumerate() {
            let at = offset + i;
            self.pages[at / 16][at % 16] = byte;
        }
        self.next += bytes.len();
        Ok(())
    }
}

#[test]
fn a_flash_sink_gets_contiguous_writes_and_too_large_blobs_are_refused() {
    let blob = plan();
    let mut rx = BlobReceiver::new(Flash {
        pages: [[0; 16]; 8],
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
    // Not a chunk frame at all.
    assert_eq!(rx.push(b"DMCU"), Err(ReceiveError::Frame));
}
