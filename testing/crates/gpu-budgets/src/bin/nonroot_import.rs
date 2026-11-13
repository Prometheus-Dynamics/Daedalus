//! The check that a process which isn't root, and so can't open a dma-heap, imports a dma-buf it
//! is handed. On a board the heaps are root's and the consumers aren't root.
//!
//! Run as root: `nonroot_import <uid> <gid>`. The parent stands in for the producer. For each
//! kind of fd, writable and read-only, it allocates a buffer from the system heap and starts
//! itself again as the given user, with the buffer as the child's standard input. The child
//! checks that it isn't root and can't open the heap, opens the device, runs the planned import
//! on the buffer, and prints what happened. Nothing here changes what user the parent is.
//!
//! A third child is handed one buffer with three values in it, each at its own offset, and
//! imports each where it is, finds each again when the buffer is written anew, and imports one a
//! second time beside its first import. `nonroot_import root` runs that case in this process,
//! as the producer itself would.
#![allow(clippy::print_stdout, clippy::print_stderr)] // the lines printed are this program's output

#[cfg(not(target_os = "linux"))]
fn main() {
    println!("nonroot_import: skipped, dma-bufs are a Linux interface");
}

#[cfg(target_os = "linux")]
fn main() -> std::process::ExitCode {
    linux::main()
}

#[cfg(target_os = "linux")]
mod linux {
    use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
    use std::os::unix::process::CommandExt;
    use std::process::{Command, ExitCode, Stdio};
    use std::sync::Arc;

    use daedalus_abi::testing::{release, run_once};
    use daedalus_abi::{Slot, Slots, Step, StepCx, StepEnd};
    use daedalus_backend::{Backend, CompileCx};
    use daedalus_dma_sys::{Access, DmaBufRef, DmaHeap};
    use daedalus_runner::{drop_owned, put_typed, take_typed};
    use daedalus_types::{SpaceIndex, TypeExpr, TypeIndex, TypeKey, Typed};
    use daedalus_wgpu::{DmaBufBacked, Options, Storage, WgpuBackend};

    const BYTES: usize = 4096;
    /// Room past the value for a device's padding: V3D wants a page and 256 bytes.
    const ROOM: usize = 8192;

    struct Writable(OwnedFd);
    struct OnlyRead(OwnedFd);

    impl Typed for Writable {
        const KEY: TypeKey = TypeKey::new("check:handed_writable");
        const EXPR: TypeExpr = TypeExpr::Opaque(TypeKey::new("check:handed_writable"));
    }
    impl Typed for OnlyRead {
        const KEY: TypeKey = TypeKey::new("check:handed_only_read");
        const EXPR: TypeExpr = TypeExpr::Opaque(TypeKey::new("check:handed_only_read"));
    }
    impl DmaBufBacked for Writable {
        fn dmabuf(&self) -> Option<(BorrowedFd<'_>, u64)> {
            Some((self.0.as_fd(), BYTES as u64))
        }
    }
    impl DmaBufBacked for OnlyRead {
        const READ_ONLY: bool = true;
        fn dmabuf(&self) -> Option<(BorrowedFd<'_>, u64)> {
            Some((self.0.as_fd(), BYTES as u64))
        }
    }

    /// How far apart the values of the offsets case are, and how many there are.
    const PITCH: usize = 8192;
    const PARTS: usize = 3;

    /// One value of several in a dma-buf.
    struct Part(OwnedFd, u64);

    impl Typed for Part {
        const KEY: TypeKey = TypeKey::new("check:handed_part");
        const EXPR: TypeExpr = TypeExpr::Opaque(TypeKey::new("check:handed_part"));
    }
    impl DmaBufBacked for Part {
        fn dmabuf(&self) -> Option<(BorrowedFd<'_>, u64)> {
            Some((self.0.as_fd(), BYTES as u64))
        }
        fn offset(&self) -> u64 {
            self.1
        }
    }

    /// The bytes of value `part` in round `round`.
    fn part_pattern(part: usize, round: usize) -> Vec<u8> {
        (0..BYTES)
            .map(|at| (at * 7 + part * 31 + round * 101 + 3) as u8)
            .collect()
    }

    /// Writes every value of a round into the buffer, as its producer would.
    fn write_round(fd: BorrowedFd<'_>, round: usize) -> Result<(), daedalus_dma_sys::Error> {
        let view = DmaBufRef::from_fd(fd);
        let map = view.map()?;
        view.begin_cpu_access(Access::Write)?;
        let mut written = Ok(());
        for part in 0..PARTS {
            if written.is_ok() {
                written = map.write((part * PITCH) as u64, &part_pattern(part, round));
            }
        }
        view.end_cpu_access(Access::Write)?;
        written
    }

    fn pattern() -> Vec<u8> {
        (0..BYTES).map(|at| (at * 7 + 3) as u8).collect()
    }

    /// The real and effective user ids of this process, from the kernel's own account of it.
    fn uids() -> Option<(u32, u32)> {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        let line = status.lines().find_map(|line| line.strip_prefix("Uid:"))?;
        let mut ids = line.split_whitespace().map(str::parse::<u32>);
        Some((ids.next()?.ok()?, ids.next()?.ok()?))
    }

    pub fn main() -> ExitCode {
        let args: Vec<String> = std::env::args().collect();
        match args.get(1).map(String::as_str) {
            Some("child") if args.get(2).is_some_and(|kind| kind == "offsets") => offsets_child(),
            Some("child") => child(args.get(2).is_some_and(|kind| kind == "read-only")),
            Some("root") => offsets_root(),
            Some(uid) => match (uid.parse(), args.get(2).and_then(|gid| gid.parse().ok())) {
                (Ok(uid), Some(gid)) => parent(uid, gid),
                _ => usage(),
            },
            None => usage(),
        }
    }

    fn usage() -> ExitCode {
        eprintln!("usage, as root: nonroot_import <uid> <gid>, or: nonroot_import root");
        ExitCode::from(2)
    }

    /// The producer: root, with the heap. Hands each kind of buffer to a child that is neither.
    fn parent(uid: u32, gid: u32) -> ExitCode {
        if uids().is_none_or(|(_, effective)| effective != 0) {
            eprintln!("nonroot_import: the parent has to be root, to allocate from the heap");
            return ExitCode::from(2);
        }
        if uid == 0 {
            eprintln!("nonroot_import: the child's uid has to be a user that isn't root");
            return ExitCode::from(2);
        }
        let heap = match DmaHeap::open("system") {
            Ok(heap) => heap,
            Err(error) => {
                eprintln!("nonroot_import: /dev/dma_heap/system: {error}");
                return ExitCode::from(2);
            }
        };
        let own = match std::env::current_exe() {
            Ok(own) => own,
            Err(error) => {
                eprintln!("nonroot_import: this program's own path: {error}");
                return ExitCode::from(2);
            }
        };
        let mut failed = false;
        for read_only in [false, true] {
            let len = (BYTES + ROOM) as u64;
            let made = if read_only {
                heap.allocate_read_only(len)
            } else {
                heap.allocate(len).and_then(|buf| {
                    let map = buf.as_ref().map()?;
                    buf.as_ref().begin_cpu_access(Access::Write)?;
                    map.write(0, &pattern())?;
                    buf.as_ref().end_cpu_access(Access::Write)?;
                    Ok(buf)
                })
            };
            let buf = match made {
                Ok(buf) => buf,
                Err(error) => {
                    eprintln!("nonroot_import: a buffer from the heap: {error}");
                    return ExitCode::from(2);
                }
            };
            let kind = if read_only { "read-only" } else { "writable" };
            let status = Command::new(&own)
                .args(["child", kind])
                .stdin(Stdio::from(buf.into_fd()))
                .uid(uid)
                .gid(gid)
                .status();
            match status {
                Ok(status) if status.success() => {}
                Ok(status) => {
                    println!("{kind}: the child failed ({status})");
                    failed = true;
                }
                Err(error) => {
                    println!("{kind}: the child couldn't be started: {error}");
                    failed = true;
                }
            }
        }
        // And one buffer of several values, each at its own offset.
        let status = heap
            .allocate(((PARTS + 2) * PITCH) as u64)
            .and_then(|buf| {
                write_round(buf.as_ref().as_fd(), 0)?;
                Ok(buf)
            })
            .map(|buf| {
                Command::new(&own)
                    .args(["child", "offsets"])
                    .stdin(Stdio::from(buf.into_fd()))
                    .uid(uid)
                    .gid(gid)
                    .status()
            });
        match status {
            Ok(Ok(status)) if status.success() => {}
            Ok(Ok(status)) => {
                println!("offsets: the child failed ({status})");
                failed = true;
            }
            Ok(Err(error)) => {
                println!("offsets: the child couldn't be started: {error}");
                failed = true;
            }
            Err(error) => {
                eprintln!("nonroot_import: a buffer from the heap: {error}");
                return ExitCode::from(2);
            }
        }
        if failed {
            ExitCode::FAILURE
        } else {
            ExitCode::SUCCESS
        }
    }

    fn cx() -> CompileCx<'static> {
        CompileCx {
            types: &[],
            consts: &[],
            spaces: &[],
            bodies: &[],
            edges: &[],
            host_inputs: &[],
            host_outputs: &[],
            fifo_capacity: 64,
            merge_allowed: true,
            freeze_params: false,
            sharing: &[],
        }
    }

    /// The planned import of a `T`, as a step.
    fn import_step<T: DmaBufBacked>(backend: &WgpuBackend) -> Result<Step, String> {
        let id = format!("wgpu.import:{}", T::KEY.as_str());
        let transfers = backend.transfers();
        let transfer = transfers
            .iter()
            .find(|transfer| transfer.id == id)
            .ok_or("the import isn't planned")?;
        (transfer.make)(&cx()).map_err(|refusal| format!("{refusal:?}"))
    }

    /// Runs `step` on `value`, and gives the device buffer it made.
    fn run_import<T: DmaBufBacked>(step: &mut Step, value: T) -> Result<Arc<Storage>, String> {
        let mut cells = [Slot::EMPTY, Slot::EMPTY];
        put_typed(&mut cells[0], TypeIndex(0), SpaceIndex(0), Arc::new(value));
        let mut context = StepCx::new();
        let end = {
            let mut slots = Slots::of(&mut cells);
            run_once(step, &mut slots, &mut context)
        };
        let result = if matches!(end, StepEnd::Done) {
            take_typed::<Storage>(&mut cells[1])
                .ok_or_else(|| "the step gave no device buffer".to_string())
        } else {
            Err(format!("the step faulted: {}", context.reason()))
        };
        drop_owned(cells[0]);
        result
    }

    /// The device's bytes for a value.
    fn read(storage: &Storage) -> Result<Vec<u8>, String> {
        let mut bytes = vec![1; BYTES];
        storage
            .read_into(0, &mut bytes)
            .map_err(|error| error.to_string())?;
        Ok(bytes)
    }

    /// Runs the planned import of `value`, and gives how it ended with the device's bytes.
    fn import<T: DmaBufBacked>(backend: &WgpuBackend, value: T) -> Result<Vec<u8>, String> {
        let mut step = import_step::<T>(backend)?;
        let result = run_import(&mut step, value).and_then(|storage| read(&storage));
        release(step);
        result
    }

    /// The offsets case as the producer itself: root, with the heap.
    fn offsets_root() -> ExitCode {
        let made = DmaHeap::open("system").and_then(|heap| {
            let buf = heap.allocate(((PARTS + 2) * PITCH) as u64)?;
            write_round(buf.as_ref().as_fd(), 0)?;
            Ok(buf.into_fd())
        });
        match made {
            Ok(fd) => offsets(&fd),
            Err(error) => {
                eprintln!("nonroot_import: a buffer from the heap: {error}");
                ExitCode::from(2)
            }
        }
    }

    /// The offsets case as a consumer that isn't root, with the buffer on its standard input.
    fn offsets_child() -> ExitCode {
        let say = |what: &str| println!("offsets: {what}");
        if uids().is_none_or(|(real, effective)| real == 0 || effective == 0) {
            say("the child is still root: nothing was checked");
            return ExitCode::FAILURE;
        }
        if DmaHeap::open("system").is_ok() {
            say("the child can open the heap: this isn't the case being checked");
            return ExitCode::FAILURE;
        }
        match std::io::stdin().as_fd().try_clone_to_owned() {
            Ok(fd) => offsets(&fd),
            Err(error) => {
                say(&format!("no fd on standard input: {error}"));
                ExitCode::FAILURE
            }
        }
    }

    /// Three values at offsets in the one dma-buf `fd`: each imported where it is, each found
    /// again when the buffer is written anew, and one imported a second time beside the first.
    fn offsets(fd: &OwnedFd) -> ExitCode {
        let uid = uids().map_or(u32::MAX, |(_, effective)| effective);
        let backend = WgpuBackend::open(Options::default()).with_dmabuf::<Part>();
        if let Err(error) = backend.start() {
            println!("offsets: uid {uid} can't open the device: {error}");
            return ExitCode::FAILURE;
        }
        let caps = backend.caps();
        let name = caps.as_ref().map_or("?", |caps| caps.adapter.name.as_str());
        println!("offsets: uid {uid} on {name}");
        let moved = || {
            backend
                .transfer_stats()
                .map_or((0, 0), |stats| (stats.imports, stats.uploads))
        };
        let part = |at: usize| fd.try_clone().map(|fd| Part(fd, (at * PITCH) as u64));
        let how = |before: (u64, u64)| {
            let now = moved();
            match (now.0 - before.0, now.1 - before.1) {
                (1, 0) => "imported in place",
                (0, 1) => "copied",
                (0, 0) => "found again, nothing imported or copied",
                _ => "something else",
            }
        };
        let mut ok = true;
        let mut check = |what: String, how: &str, wanted: &str, right: bool| {
            let fine = right && how == wanted;
            println!(
                "offsets: {what}: {how}, bytes {}{}",
                if right { "right" } else { "WRONG" },
                if fine { "" } else { " -- NOT AS EXPECTED" }
            );
            ok &= fine;
        };
        let mut run = || -> Result<(), String> {
            let mut step = import_step::<Part>(&backend)?;
            let mut held = Vec::new();
            for round in 0..2 {
                if round == 1 {
                    write_round(fd.as_fd(), 1).map_err(|error| error.to_string())?;
                }
                for at in 0..PARTS {
                    let before = moved();
                    let value = part(at).map_err(|error| error.to_string())?;
                    let storage = run_import(&mut step, value)?;
                    let right = read(&storage)? == part_pattern(at, round);
                    let wanted = if round == 0 {
                        "imported in place"
                    } else {
                        "found again, nothing imported or copied"
                    };
                    check(
                        format!("round {round}, value {at} at offset {}", at * PITCH),
                        how(before),
                        wanted,
                        right,
                    );
                    held.push(storage);
                }
            }
            // A second step has kept nothing, so it imports value 0 again while the first
            // import of it is still held: two imports of one dma-buf at once.
            let mut second = import_step::<Part>(&backend)?;
            let before = moved();
            let again = run_import(&mut second, part(0).map_err(|error| error.to_string())?)?;
            let right = read(&again)? == part_pattern(0, 1);
            check(
                "value 0 a second time, beside its first import".to_string(),
                how(before),
                "imported in place",
                right,
            );
            let first = read(&held[0])? == part_pattern(0, 1);
            check(
                "the first import of value 0, still held".to_string(),
                "kept",
                "kept",
                first,
            );
            drop(again);
            release(second);
            let alone = read(&held[0])? == part_pattern(0, 1);
            check(
                "the first import, after the second is gone".to_string(),
                "kept",
                "kept",
                alone,
            );
            drop(held);
            release(step);
            Ok(())
        };
        if let Err(error) = run() {
            println!("offsets: {error}");
            return ExitCode::FAILURE;
        }
        if ok {
            println!("offsets: PASS");
            ExitCode::SUCCESS
        } else {
            println!("offsets: FAIL");
            ExitCode::FAILURE
        }
    }

    /// The consumer: not root, no heap, and one dma-buf on its standard input.
    fn child(read_only: bool) -> ExitCode {
        let kind = if read_only { "read-only" } else { "writable" };
        let say = |what: &str| println!("{kind}: {what}");
        let Some((real, effective)) = uids() else {
            say("couldn't read its own user ids");
            return ExitCode::FAILURE;
        };
        if real == 0 || effective == 0 {
            say("the child is still root: nothing was checked");
            return ExitCode::FAILURE;
        }
        if DmaHeap::open("system").is_ok() {
            say("the child can open the heap: this isn't the case being checked");
            return ExitCode::FAILURE;
        }
        let fd = match std::io::stdin().as_fd().try_clone_to_owned() {
            Ok(fd) => fd,
            Err(error) => {
                say(&format!("no fd on standard input: {error}"));
                return ExitCode::FAILURE;
            }
        };
        if let Err(error) = DmaBufRef::from_fd(fd.as_fd()).len() {
            say(&format!("standard input isn't a dma-buf: {error}"));
            return ExitCode::FAILURE;
        }
        let backend = WgpuBackend::open(Options::default())
            .with_dmabuf::<Writable>()
            .with_dmabuf::<OnlyRead>();
        if let Err(error) = backend.start() {
            say(&format!("uid {effective} can't open the device: {error}"));
            return ExitCode::FAILURE;
        }
        let caps = backend.caps();
        let name = caps.as_ref().map_or("?", |caps| caps.adapter.name.as_str());
        let before = backend.dmabuf_import(read_only);
        let uploads = backend.transfer_stats().map_or(0, |stats| stats.uploads);
        let bytes = if read_only {
            import(&backend, OnlyRead(fd))
        } else {
            import(&backend, Writable(fd))
        };
        let after = backend.dmabuf_import(read_only);
        let copied = backend.transfer_stats().map_or(0, |stats| stats.uploads) - uploads;
        let expected = if read_only { vec![0; BYTES] } else { pattern() };
        let (right, how) = match &bytes {
            Ok(bytes) => (*bytes == expected, String::from("bytes read back")),
            Err(error) => (false, error.clone()),
        };
        say(&format!(
            "uid {effective} on {name}: before {before:?}, after {after:?}, copies {copied}, {how}{}",
            if bytes.is_ok() && !right {
                ", and they are WRONG"
            } else {
                ""
            }
        ));
        // Unknown before, since this process can try nothing; settled after; and the right bytes.
        if before.is_none() && after.is_some() && right {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        }
    }
}
