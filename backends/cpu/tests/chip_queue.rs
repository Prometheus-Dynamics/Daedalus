//! The fixed queues generated firmware uses.

#![cfg(feature = "chip")]

use daedalus_cpu::chip::{ChipError, Overflow, Queue, QueueFull, push};

fn drain<const N: usize>(queue: &mut Queue<u32, N>) -> Vec<u32> {
    std::iter::from_fn(|| queue.pop()).collect()
}

#[test]
fn values_leave_in_the_order_they_came() {
    let mut queue = Queue::<u32, 3>::new();
    for value in [1, 2] {
        queue.push(value, Overflow::Error).unwrap();
    }
    assert_eq!(queue.pop(), Some(1));
    // Goes round the end of the buffer.
    for value in [3, 4] {
        queue.push(value, Overflow::Error).unwrap();
    }
    assert_eq!(drain(&mut queue), [2, 3, 4]);
    assert_eq!(queue.pop(), None);
}

#[test]
fn latest_only_keeps_the_newest() {
    let mut queue = Queue::<u32, 1>::new();
    for value in [1, 2, 3] {
        queue.push(value, Overflow::DropOldest).unwrap();
    }
    assert_eq!(drain(&mut queue), [3]);
}

#[test]
fn a_full_queue_drops_the_oldest() {
    let mut queue = Queue::<u32, 2>::new();
    for value in [1, 2, 3, 4, 5] {
        queue.push(value, Overflow::DropOldest).unwrap();
    }
    assert_eq!(drain(&mut queue), [4, 5]);
}

#[test]
fn a_full_queue_drops_the_newest() {
    let mut queue = Queue::<u32, 2>::new();
    for value in [1, 2, 3, 4, 5] {
        queue.push(value, Overflow::DropNewest).unwrap();
    }
    assert_eq!(drain(&mut queue), [1, 2]);
}

#[test]
fn a_full_queue_can_refuse() {
    let mut queue = Queue::<u32, 2>::new();
    queue.push(1, Overflow::Error).unwrap();
    queue.push(2, Overflow::Error).unwrap();
    assert_eq!(queue.push(3, Overflow::Error), Err(QueueFull));
    assert_eq!(
        push(&mut queue, 3, Overflow::Error, 7),
        Err(ChipError::QueueFull { edge: 7 })
    );
    assert_eq!(drain(&mut queue), [1, 2]);
}

#[test]
fn a_queue_can_be_a_static() {
    static QUEUE: Queue<u32, 4> = Queue::new();
    assert!(QUEUE.is_empty());
}
