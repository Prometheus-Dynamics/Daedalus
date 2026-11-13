//! The check that a process which isn't root, and so can't open a dma-heap, imports a dma-buf it
//! is handed. On a board the heaps are root's and the consumers aren't root.
//!
//! Run as root: `nonroot_import <uid> <gid>`. The parent stands in for the producer. For each
//! kind of fd, writable and read-only, it allocates a buffer from the system heap and starts
//! itself again as the given user, with the buffer as the child's standard input. The child
//! checks that it isn't root and can't open the heap, opens the device, runs the planned import
//! on the buffer, and prints what happened. Nothing here changes what user the parent is.
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
    use daedalus_abi::{Slot, Slots, StepCx, StepEnd};
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
            Some("child") => child(args.get(2).is_some_and(|kind| kind == "read-only")),
            Some(uid) => match (uid.parse(), args.get(2).and_then(|gid| gid.parse().ok())) {
                (Ok(uid), Some(gid)) => parent(uid, gid),
                _ => usage(),
            },
            None => usage(),
        }
    }

    fn usage() -> ExitCode {
        eprintln!("usage, as root: nonroot_import <uid> <gid>");
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

    /// Runs the planned import of `value`, and gives how it ended with the device's bytes.
    fn import<T: DmaBufBacked>(backend: &WgpuBackend, value: T) -> Result<Vec<u8>, String> {
        let id = format!("wgpu.import:{}", T::KEY.as_str());
        let transfers = backend.transfers();
        let transfer = transfers
            .iter()
            .find(|transfer| transfer.id == id)
            .ok_or("the import isn't planned")?;
        let mut step = (transfer.make)(&cx()).map_err(|refusal| format!("{refusal:?}"))?;
        let mut cells = [Slot::EMPTY, Slot::EMPTY];
        put_typed(&mut cells[0], TypeIndex(0), SpaceIndex(0), Arc::new(value));
        let mut context = StepCx::new();
        let end = {
            let mut slots = Slots::of(&mut cells);
            run_once(&mut step, &mut slots, &mut context)
        };
        let result = if matches!(end, StepEnd::Done) {
            take_typed::<Storage>(&mut cells[1])
                .ok_or_else(|| "the step gave no device buffer".to_string())
                .and_then(|storage| {
                    let mut bytes = vec![1; BYTES];
                    storage
                        .read_into(0, &mut bytes)
                        .map_err(|error| error.to_string())?;
                    Ok(bytes)
                })
        } else {
            Err(format!("the step faulted: {}", context.reason()))
        };
        drop_owned(cells[0]);
        release(step);
        result
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
