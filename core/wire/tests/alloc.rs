//! Allocation: a length the cap refuses allocates nothing, and a warm read reuses its buffer.

use std::io::Cursor;

use daedalus_abi::counting::{CountingAlloc, counted};
use daedalus_wire::{read_packet, write_packet};

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

#[test]
fn a_length_over_the_cap_allocates_nothing() {
    let cap = 16 * 1024 * 1024;
    let over: u32 = cap + 1;
    let mut header = Cursor::new(over.to_le_bytes().to_vec());
    let mut buf = Vec::new();
    let (result, allocations) = counted(|| read_packet(&mut header, cap, &mut buf));
    assert!(result.is_err());
    assert_eq!(allocations, 0);
    assert_eq!(buf.capacity(), 0);
}

#[test]
fn a_warm_packet_read_allocates_nothing() {
    let body = vec![7u8; 64 * 1024];
    let mut wire = Vec::new();
    write_packet(&mut wire, &body, 1 << 20).unwrap();
    let mut buf = Vec::new();
    // The first read sizes the buffer; the second reuses it.
    read_packet(&mut Cursor::new(&wire), 1 << 20, &mut buf).unwrap();
    let (result, allocations) = counted(|| read_packet(&mut Cursor::new(&wire), 1 << 20, &mut buf));
    result.unwrap();
    assert_eq!(allocations, 0);
    assert_eq!(buf, body);
}

#[test]
fn a_packet_over_the_cap_is_refused_on_write() {
    let mut wire = Vec::new();
    assert!(write_packet(&mut wire, &[0u8; 9], 8).is_err());
    assert!(wire.is_empty());
}
