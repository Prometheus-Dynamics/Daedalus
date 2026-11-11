//! Each example's GPU output against the CPU's. A machine with no GPU fails these where the
//! GPU lane runs them (`DAEDALUS_GPU_REQUIRED=1`), and skips them elsewhere, as every GPU test
//! in this repository does. The binaries themselves always fail without a GPU.

use std::process::Command;

use daedalus_example_gpu::{fragment, gpu, mixed, upload_download};

/// The examples share the process's one device, and one of them counts its passes: a test at
/// a time.
static ONE: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Whether there is a GPU to run on, and the turn to use it. Without one this is a failure in
/// the GPU lane.
fn has_gpu() -> Option<std::sync::MutexGuard<'static, ()>> {
    let turn = ONE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match gpu() {
        Ok(_) => Some(turn),
        Err(error) => {
            assert!(
                std::env::var_os("DAEDALUS_GPU_REQUIRED").is_none(),
                "{error}"
            );
            None
        }
    }
}

/// The binary prints the library's line and exits 0.
fn binary_prints(path: &str, line: &str) {
    let output = Command::new(path).output().expect("the example runs");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    // Under the Vulkan validation layer, as in the GPU lane, the layer writes a line of its own
    // to the program's output when it starts. That line isn't the example's.
    let printed: String = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|printed| !printed.starts_with("Validation Layer"))
        .map(|printed| format!("{printed}\n"))
        .collect();
    assert_eq!(printed, line);
}

#[test]
fn fragment_matches_cpu() {
    let Some(_turn) = has_gpu() else { return };
    for len in [1u32, 63, 64, 65, 4096, 10_001] {
        let pixels: Vec<u32> = (0..len)
            .map(|at| at.wrapping_mul(2_654_435_761) >> 8)
            .collect();
        let (mask, passes) = fragment::on_gpu(&pixels).expect("runs");
        assert_eq!(mask, fragment::on_cpu(&pixels), "{len} pixels");
        assert_eq!(passes, 1, "three nodes, one shader");
    }
    let line = "fragment: three per-pixel nodes ran as 1 shader over 4096 pixels, 2688 are lit, as on the CPU\n";
    assert_eq!(fragment::run().as_deref(), Ok(line));
    binary_prints(env!("CARGO_BIN_EXE_fragment"), line);
}

#[test]
fn mixed_round_trip_matches_cpu() {
    let Some(_turn) = has_gpu() else { return };
    for count in [1, 2, 64, 1000, 5000] {
        assert_eq!(
            mixed::through_the_graph(count),
            Ok(mixed::on_cpu(count)),
            "{count} numbers"
        );
    }
    let line = "mixed: 5000 numbers made on the CPU, scaled on the GPU and summed on the CPU give 37512500\n";
    assert_eq!(mixed::run().as_deref(), Ok(line));
    binary_prints(env!("CARGO_BIN_EXE_mixed"), line);
}

#[test]
fn upload_download_round_trip() {
    let Some(_turn) = has_gpu() else { return };
    for len in [1u32, 3, 64, 1000, 65_536] {
        let input: Vec<u32> = (0..len).map(|at| at % 46_341).collect();
        let want: Vec<u32> = input.iter().map(|x| x * x).collect();
        assert_eq!(
            upload_download::squared_on_gpu(&input),
            Ok(want),
            "{len} words"
        );
    }
    let line =
        "upload_download: 1000 words squared on the GPU match the CPU, the last is 1000000\n";
    assert_eq!(upload_download::run().as_deref(), Ok(line));
    binary_prints(env!("CARGO_BIN_EXE_upload_download"), line);
}

#[test]
fn a_wrong_answer_is_caught() {
    // What each example compares against is the CPU's own arithmetic, so a GPU that got it
    // wrong would differ here.
    assert_eq!(mixed::on_cpu(3), 4 + 7 + 10);
    assert_eq!(fragment::on_cpu(&[0, 100, 200, 255]), [255, 255, 0, 0]);
}

/// The shader writes the round trip's result straight into memory the host reads: dma-heap memory
/// when the device imports dma-bufs, host-mapped memory when it does not. So no readback copy is
/// planned, only the map that hands the host the bytes where they are.
#[test]
fn the_result_is_written_where_the_host_reads_it() {
    let Some(_turn) = has_gpu() else { return };
    let gpu = daedalus_example_gpu::gpu().expect("a device");
    let imports = gpu.caps().expect("open").dma_heap;
    let route = upload_download::readback_route(&gpu).expect("plans");
    // The typed map's id carries the payload's key, so the id is matched by its prefix.
    let map = if imports {
        "wgpu.map.dmaheap:"
    } else {
        "wgpu.map:"
    };
    assert_eq!(route.len(), 1, "{route:?}");
    assert!(route[0].starts_with(map), "{route:?}");
}

/// On Vulkan under Linux the host's result is the shader's buffer itself, read where it is.
#[test]
fn the_host_reads_the_result_in_place() {
    let Some(_turn) = has_gpu() else { return };
    let gpu = daedalus_example_gpu::gpu().expect("a device");
    let vulkan = gpu.caps().expect("open").adapter.api == daedalus_wgpu::Api::Vulkan;
    let input: Vec<u32> = (1..=1000).collect();
    let words = upload_download::squared_words(&input).expect("runs");
    assert_eq!(
        words.to_vec(),
        input.iter().map(|x| x * x).collect::<Vec<_>>()
    );
    if cfg!(target_os = "linux") && vulkan {
        assert!(words.is_in_place(), "the result was copied out");
    }
}
