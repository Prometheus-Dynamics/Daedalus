//! The interface's contract, on a toy accelerator defined here (it adds two byte vectors on a
//! thread): load, slots, buffers, submit after a wait, fence, read back, release, and the errors.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use daedalus_accelerator::{
    Accelerator, AcceleratorError, AcceleratorId, AcceleratorInfo, Access, Binding, BufferHandle,
    BufferSource, CpuFence, FenceHandle, ProgramDesc, ProgramHandle, ProgramInfo, SlotInfo,
    Submission, Wait, validate_bindings,
};

/// Programs are `test.add-u8` images holding a little-endian `u32` length `n`: slots 0 and 1
/// (inputs, `n` bytes each) are added into slot 2.
struct Adder {
    id: AcceleratorId,
    next: AtomicU64,
    programs: Mutex<HashMap<u64, usize>>,
    buffers: Arc<Mutex<HashMap<u64, Vec<u8>>>>,
}

impl Adder {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            id: AcceleratorId::next(),
            next: AtomicU64::new(1),
            programs: Mutex::default(),
            buffers: Arc::default(),
        })
    }

    fn live_buffers(&self) -> usize {
        self.buffers.lock().unwrap().len()
    }
}

impl Accelerator for Adder {
    fn id(&self) -> AcceleratorId {
        self.id
    }

    fn info(&self) -> AcceleratorInfo {
        AcceleratorInfo {
            name: "adder".into(),
            kind: "test".into(),
            program_formats: vec!["test.add-u8".into()],
            ..AcceleratorInfo::default()
        }
    }

    fn load_program(&self, desc: &ProgramDesc<'_>) -> Result<ProgramHandle, AcceleratorError> {
        if desc.format != "test.add-u8" {
            return Err(AcceleratorError::unsupported(format!(
                "format {}",
                desc.format
            )));
        }
        let n: [u8; 4] = desc
            .image
            .try_into()
            .map_err(|_| AcceleratorError::Program {
                reason: "the image is a u32 length".into(),
            })?;
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        self.programs
            .lock()
            .unwrap()
            .insert(id, u32::from_le_bytes(n) as usize);
        Ok(ProgramHandle::new(self.id, id, desc.image.len(), None))
    }

    fn program_info(&self, program: &ProgramHandle) -> Result<ProgramInfo, AcceleratorError> {
        let id = program.check_owner(self.id)?;
        let n = self.programs.lock().unwrap()[&id];
        let slot = |slot, name: &str| SlotInfo {
            slot,
            name: name.into(),
            len: n,
        };
        Ok(ProgramInfo {
            inputs: vec![slot(0, "a"), slot(1, "b")],
            outputs: vec![slot(2, "sum")],
        })
    }

    fn buffer(&self, source: BufferSource<'_>) -> Result<BufferHandle, AcceleratorError> {
        let bytes = match source {
            BufferSource::Allocate { len } => vec![0; len],
            BufferSource::Copy(bytes) => bytes.to_vec(),
            #[cfg(unix)]
            BufferSource::DmaBuf { .. } => {
                return Err(AcceleratorError::unsupported("dmabuf import"));
            }
        };
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let len = bytes.len();
        self.buffers.lock().unwrap().insert(id, bytes);
        let buffers = Arc::downgrade(&self.buffers);
        let release = Box::new(move |id| {
            if let Some(buffers) = buffers.upgrade() {
                buffers.lock().unwrap().remove(&id);
            }
        });
        Ok(BufferHandle::new(self.id, id, len, Some(release)))
    }

    fn read(
        &self,
        buffer: &BufferHandle,
        offset: usize,
        out: &mut [u8],
    ) -> Result<(), AcceleratorError> {
        let id = buffer.check_owner(self.id)?;
        let buffers = self.buffers.lock().unwrap();
        let bytes = &buffers[&id];
        let range = bytes
            .get(offset..offset + out.len())
            .ok_or(AcceleratorError::OutOfRange {
                offset,
                len: out.len(),
                buffer: bytes.len(),
            })?;
        out.copy_from_slice(range);
        Ok(())
    }

    fn submit(&self, submission: Submission<'_>) -> Result<FenceHandle, AcceleratorError> {
        let info = self.program_info(submission.program)?;
        validate_bindings(&info, submission.bindings)?;
        let mut ids = [0; 3];
        for binding in submission.bindings {
            ids[binding.slot as usize] = binding.buffer.check_owner(self.id)?;
        }
        let fence = CpuFence::new();
        let (done, buffers, waits) = (fence.clone(), self.buffers.clone(), submission.waits);
        // Keep the buffers alive until the work completes, as a device would.
        let keep: Vec<BufferHandle> = submission
            .bindings
            .iter()
            .map(|b| b.buffer.clone())
            .collect();
        std::thread::spawn(move || {
            let outcome = waits
                .iter()
                .try_for_each(|wait| wait.wait_on_cpu(None))
                .map(|()| {
                    let mut buffers = buffers.lock().unwrap();
                    let sum: Vec<u8> = buffers[&ids[0]]
                        .iter()
                        .zip(&buffers[&ids[1]])
                        .map(|(a, b)| a.wrapping_add(*b))
                        .collect();
                    buffers.get_mut(&ids[2]).unwrap()[..sum.len()].copy_from_slice(&sum);
                });
            drop(keep);
            done.signal(outcome);
        });
        Ok(fence)
    }
}

const N: usize = 6;

fn program(npu: &dyn Accelerator) -> ProgramHandle {
    npu.load_program(&ProgramDesc::new("test.add-u8", &(N as u32).to_le_bytes()))
        .expect("load")
}

#[test]
fn a_submission_runs_after_its_wait_and_signals_its_fence() {
    let adder = Adder::new();
    let npu: Arc<dyn Accelerator> = adder.clone();
    let program = program(&*npu);
    let info = npu.program_info(&program).unwrap();
    assert_eq!((info.inputs.len(), info.outputs[0].len), (2, N));

    let a = npu
        .buffer(BufferSource::Copy(&[1, 2, 3, 4, 5, 250]))
        .unwrap();
    let b = npu
        .buffer(BufferSource::Copy(&[10, 20, 30, 40, 50, 10]))
        .unwrap();
    let sum = npu.buffer(BufferSource::Allocate { len: N }).unwrap();
    let bindings = [
        Binding::new(0, a, Access::Read),
        Binding::new(1, b, Access::Read),
        Binding::new(2, sum.clone(), Access::Write),
    ];
    let upstream = CpuFence::new();
    let fence = npu
        .submit(Submission::new(&program, &bindings).after(Wait::Fence(upstream.clone())))
        .unwrap();
    std::thread::sleep(Duration::from_millis(20));
    assert!(
        !fence.is_signaled(),
        "the program waits for its upstream fence"
    );
    assert_eq!(
        fence.wait(Some(Duration::from_millis(5))),
        Err(AcceleratorError::Timeout {
            after: Duration::from_millis(5)
        })
    );
    upstream.signal(Ok(()));
    fence.wait(Some(Duration::from_secs(5))).unwrap();
    let mut out = [0; N];
    npu.read(&sum, 0, &mut out).unwrap();
    assert_eq!(out, [11, 22, 33, 44, 55, 4]);

    drop(bindings);
    assert_eq!(adder.live_buffers(), 1, "released with their last handles");
    drop(sum);
    assert_eq!(adder.live_buffers(), 0);
}

#[test]
fn an_upstream_failure_fails_the_submission() {
    let npu = Adder::new();
    let program = program(&*npu);
    let buffer = |len| npu.buffer(BufferSource::Allocate { len }).unwrap();
    let bindings = [
        Binding::new(0, buffer(N), Access::Read),
        Binding::new(1, buffer(N), Access::Read),
        Binding::new(2, buffer(N), Access::ReadWrite),
    ];
    let upstream = CpuFence::new();
    let fence = npu
        .submit(Submission::new(&program, &bindings).after(Wait::Fence(upstream.clone())))
        .unwrap();
    upstream.signal(Err(AcceleratorError::device("camera timeout")));
    assert_eq!(
        fence.wait(None),
        Err(AcceleratorError::device("camera timeout"))
    );
}

#[test]
fn bad_programs_bindings_handles_and_reads_are_refused() {
    let npu = Adder::new();
    assert!(matches!(
        npu.load_program(&ProgramDesc::new("rknn", &[])),
        Err(AcceleratorError::Unsupported { .. })
    ));
    assert!(matches!(
        npu.load_program(&ProgramDesc::new("test.add-u8", &[1])),
        Err(AcceleratorError::Program { .. })
    ));
    let program = program(&*npu);
    let buffer = |len| npu.buffer(BufferSource::Allocate { len }).unwrap();
    let slot = |result: Result<FenceHandle, AcceleratorError>| match result {
        Err(AcceleratorError::Binding { slot, reason }) => (slot, reason),
        other => panic!("expected a binding error, got {:?}", other.map(|_| ())),
    };
    let submit = |bindings: &[Binding]| npu.submit(Submission::new(&program, bindings));

    let ok = [
        Binding::new(0, buffer(N), Access::Read),
        Binding::new(1, buffer(N), Access::Read),
        Binding::new(2, buffer(N), Access::Write),
    ];
    assert_eq!(slot(submit(&ok[..2])).0, 2, "an unbound output");
    let mut small = ok.clone();
    small[1].buffer = buffer(N - 1);
    assert!(slot(submit(&small)).1.contains("needs 6 bytes"));
    let mut read_only_output = ok.clone();
    read_only_output[2].access = Access::Read;
    assert_eq!(slot(submit(&read_only_output)).0, 2);
    let mut twice = ok.clone();
    twice[1].slot = 0;
    assert_eq!(slot(submit(&twice)).1, "bound twice");
    let mut unknown = ok.clone();
    unknown[2].slot = 9;
    assert_eq!(slot(submit(&unknown)).0, 9);

    let other = Adder::new();
    let foreign = other.buffer(BufferSource::Allocate { len: N }).unwrap();
    let mut mixed = ok.clone();
    mixed[0].buffer = foreign.clone();
    assert!(matches!(
        submit(&mixed),
        Err(AcceleratorError::ForeignHandle)
    ));
    assert!(matches!(
        npu.read(&foreign, 0, &mut [0; 1]),
        Err(AcceleratorError::ForeignHandle)
    ));
    assert_eq!(
        npu.read(&ok[0].buffer, 4, &mut [0; 4]),
        Err(AcceleratorError::OutOfRange {
            offset: 4,
            len: 4,
            buffer: N
        })
    );
    #[cfg(unix)]
    {
        let fd = std::fs::File::open("/dev/null").unwrap().into();
        assert!(matches!(
            npu.buffer(BufferSource::DmaBuf {
                fd,
                offset: 0,
                len: N,
                acquire: None
            }),
            Err(AcceleratorError::Unsupported { .. })
        ));
    }
}
