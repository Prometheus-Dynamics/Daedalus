#![cfg(feature = "std")]

use super::*;
use crate::Regions;

struct Boxes(Vec<Box<[u8]>>);

impl Regions for Boxes {
    fn region(&self, index: usize) -> Option<&[u8]> {
        self.0.get(index).map(|region| &region[..])
    }
}

impl WritableRegions for Boxes {
    fn region_mut(&mut self, index: usize) -> Option<&mut [u8]> {
        self.0.get_mut(index).map(|region| &mut region[..])
    }
}

const SIZE: usize = 1024;

fn held(count: usize) -> Arc<HeldRegions<Boxes>> {
    let regions = (0..count)
        .map(|_| vec![0u8; SIZE].into_boxed_slice())
        .collect();
    HeldRegions::writable(Boxes(regions), count)
}

fn counts(leased: usize, sealed: usize, abandoned: usize) -> LeaseCounts {
    LeaseCounts {
        leased,
        sealed,
        abandoned,
    }
}

#[test]
fn a_lease_is_written_then_sealed_into_a_hand_over_with_no_copy() {
    let held = held(2);
    let mut lease = held.lease(100, 0).expect("room");
    let at = lease.bytes_mut().as_ptr();
    lease.bytes_mut()[..3].copy_from_slice(&[1, 2, 3]);
    assert_eq!(held.holders(0), 1, "the lease holds its region");
    let value = lease.seal(3).expect("inside the room");
    assert_eq!(value.bytes(), &[1, 2, 3]);
    assert_eq!(
        value.bytes().as_ptr(),
        at,
        "the hand-over is the room itself"
    );
    assert_eq!(held.locate(&value), Some((0, 0)));
    assert_eq!(held.lease_counts(), counts(1, 1, 0));
    drop(value);
    assert_eq!(held.holders(0), 0, "let go, the region is free");
}

#[test]
fn a_held_value_leaves_the_next_to_another_region_and_leases_follow_when_all_are_held() {
    let held = held(2);
    let first = held.lease(100, 0).expect("room").seal(100).expect("sealed");
    assert_eq!(held.locate(&first), Some((0, 0)));
    let second = held.lease(100, 4).expect("room");
    let (region, start) = second.place();
    assert_eq!(
        region, 1,
        "the first is still held: the next goes to a free region"
    );
    assert_eq!((start + 4) % ALIGN, 0, "its items on a boundary");
    let third = held.lease(10, 0).expect("room");
    assert_eq!(
        third.place().0,
        1,
        "every other region held: after the last lease"
    );
    assert!(
        third.place().1 >= start + 100,
        "not over the second while it is held"
    );
    drop((second, third));
    let again = held.lease(10, 0).expect("room");
    assert_eq!(
        again.place(),
        (1, 0),
        "nothing holds it: from the front again"
    );
    drop((first, again));
}

#[test]
fn a_full_region_moves_leases_to_a_free_one_and_a_held_one_is_never_reused() {
    let held = held(2);
    let big = held.lease(SIZE - 64, 0).expect("room");
    let next = held.lease(128, 0).expect("room in the other region");
    assert_eq!(next.place(), (1, 0));
    assert!(held.lease(SIZE, 0).is_none(), "both held: no room");
    drop(big);
    let after = held.lease(SIZE, 0).expect("region 0 is free again");
    assert_eq!(after.place(), (0, 0));
    drop((next, after));
}

#[test]
fn a_claimed_region_is_never_leased_and_the_leased_one_never_claimed() {
    let held = held(3);
    let lease = held.lease(16, 0).expect("room");
    let claimed = held.claim().expect("a free region");
    assert_ne!(claimed, 0, "the region leases are taken in is not claimed");
    drop(lease);
    assert_ne!(held.claim(), Some(0), "nor once its leases are gone");
    let big = held.lease(SIZE, 0).expect("region 0, from the front");
    assert_eq!(big.place(), (0, 0));
    assert!(
        held.lease(SIZE, 0).is_none(),
        "the claimed regions are not leased"
    );
    held.unclaim(claimed);
    held.unclaim(2);
    assert_eq!(held.lease(SIZE, 0).map(|lease| lease.place().0), Some(1));
    drop(big);
}

#[test]
fn every_lease_is_sealed_or_let_go_even_when_its_writer_fails_and_records_are_reused() {
    let held = held(2);
    for round in 1..=50 {
        let mut lease = held.lease(256, 0).expect("room");
        lease.bytes_mut()[0] = round as u8;
        if round % 3 == 0 {
            // The writer fails halfway: the lease is dropped unsealed.
            drop(lease);
        } else {
            let value = lease.seal(1).expect("sealed");
            assert_eq!(value.bytes(), &[round as u8]);
        }
        let LeaseCounts {
            leased,
            sealed,
            abandoned,
        } = held.lease_counts();
        assert_eq!(leased, round);
        assert_eq!(abandoned, round / 3);
        assert_eq!(sealed + abandoned, leased, "nothing is left open");
        assert_eq!(
            held.holders(0) + held.holders(1),
            0,
            "every hold given back"
        );
    }
    assert_eq!(held.records(), 1, "one record, reused every round");
}

#[test]
fn a_seal_longer_than_the_room_is_refused_and_lets_the_lease_go() {
    let held = held(1);
    let lease = held.lease(8, 0).expect("room");
    assert!(lease.seal(9).is_none());
    assert_eq!(held.holders(0), 0);
    assert_eq!(held.lease_counts(), counts(1, 0, 1));
}

#[test]
fn a_lease_grows_in_place_only_while_it_is_the_last() {
    let held = held(1);
    let mut first = held.lease(64, 0).expect("room");
    assert!(first.grow(128), "the last lease grows");
    assert_eq!(first.cap(), 128);
    let second = held.lease(64, 0).expect("room");
    assert!(second.place().1 >= 128, "after the grown room");
    assert!(!first.grow(256), "another lease follows it");
    let mut second = second;
    assert!(!second.grow(SIZE), "past the region's end");
    assert_eq!(second.cap(), 64, "a refused grow leaves it as it was");
    drop((first, second));
}

#[test]
fn a_lease_outlives_the_regions_it_came_from() {
    let held = held(1);
    let mut lease = held.lease(4, 0).expect("room");
    let weak = Arc::downgrade(&held);
    drop(held);
    lease.bytes_mut().copy_from_slice(&[9; 4]);
    let value = lease.seal(4).expect("sealed");
    assert_eq!(value.bytes(), &[9; 4], "the region is still there");
    drop(value);
    assert!(weak.upgrade().is_none(), "the last holder let it go");
}

#[test]
fn a_hand_over_from_other_regions_is_not_located() {
    let (one, two) = (held(1), held(1));
    let value = two.lease(4, 0).expect("room").seal(4).expect("sealed");
    assert_eq!(one.locate(&value), None);
    assert_eq!(two.locate(&value), Some((0, 0)));
}

#[test]
fn read_only_regions_lend_nothing() {
    let regions = (0..1).map(|_| vec![0u8; SIZE].into_boxed_slice()).collect();
    let held = HeldRegions::new(Boxes(regions), 1);
    assert!(held.lease(4, 0).is_none());
}
