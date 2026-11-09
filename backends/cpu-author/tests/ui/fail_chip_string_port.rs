//! A queue on a chip holds plain values, so a `String` port can't be stored in one.

use daedalus_cpu::chip::Queue;

fn main() {
    let _queue = Queue::<String, 4>::new();
}
