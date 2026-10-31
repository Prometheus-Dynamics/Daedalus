use super::*;
use crate::{Backend, Exempt, Kernel, Op, Registry, Status};

static OPS: [Op; 2] = [
    Op {
        id: "t.add",
        summary: "add",
        scalar: "t::add",
        per_element: true,
    },
    Op {
        id: "t.hash",
        summary: "hash",
        scalar: "t::hash",
        per_element: false,
    },
];

static CHILD: Registry = Registry {
    name: "child",
    kernels: &[Kernel::missing(
        "t.hash",
        Backend::Gpu,
        Status::NotDataParallel("each step depends on the previous"),
    )],
    ..Registry::EMPTY
};

static ROOT: Registry = Registry {
    repo: "t",
    name: "root",
    ops: &OPS,
    kernels: &[
        Kernel::implemented("t.add", Backend::Scalar, "src/add.rs:1", "add::scalar"),
        Kernel::implemented("t.add", Backend::Rvv, "src/add.rs:9", "add::rvv")
            .with_symbol("add_rvv"),
        Kernel::implemented("t.add", Backend::Neon64, "src/add.rs:20", "add::neon"),
        Kernel::missing("t.add", Backend::Gpu, Status::Impossible("tiny")),
    ],
    exempt: &[Exempt {
        path: "src/detect.rs",
        reason: "feature detection",
    }],
    children: &[&CHILD],
};

fn rec(backend: &str, vlen: Option<u32>, result: &str, kind: &str) -> String {
    format!(
        "{{\"kind\":\"{kind}\",\"op\":\"t.add\",\"backend\":\"{backend}\",\"variant\":null,\"adapter\":null,\"target\":\"x\",\"runner\":\"qemu\",\"vlen\":{},\"rev\":\"\",\"result\":\"{result}\",\"detail\":null}}\n",
        vlen.map_or("null".into(), |v| v.to_string())
    )
}

fn inventory(records: &str) -> Inventory {
    let mut inv = Inventory::default();
    inv.add_registry(&crate::dump_json(&[&ROOT])).unwrap();
    inv.add_records(records).unwrap();
    inv
}

#[test]
fn cells_need_evidence_and_rvv_needs_three_vector_lengths() {
    let partial = rec("scalar", None, "pass", "test")
        + &rec("rvv", Some(128), "pass", "test")
        + &rec("rvv", Some(256), "pass", "test");
    let inv = inventory(&partial);
    assert!(
        inv.check_registry().is_empty(),
        "{:?}",
        inv.check_registry()
    );
    assert_eq!(inv.cell("t.add", Backend::Scalar).cell, Cell::Pass);
    let rvv = inv.cell("t.add", Backend::Rvv);
    assert_eq!(rvv.cell, Cell::Untested);
    assert!(
        rvv.reason.contains("VLEN 512") && rvv.reason.contains("asm"),
        "{}",
        rvv.reason
    );
    assert_eq!(inv.cell("t.add", Backend::Neon64).cell, Cell::Untested);
    assert_eq!(inv.cell("t.add", Backend::X86Avx2).cell, Cell::Gap);
    assert_eq!(inv.cell("t.hash", Backend::Gpu).cell, Cell::Impossible);
    assert_eq!(inv.review().len(), 1, "GPU impossible on a per-element op");

    let full = partial + &rec("rvv", Some(512), "pass", "test") + &rec("rvv", None, "pass", "asm");
    let inv = inventory(&full);
    assert_eq!(inv.cell("t.add", Backend::Rvv).cell, Cell::Pass);
    let fail = full.clone() + &rec("rvv", Some(256), "fail", "test");
    assert_eq!(
        inventory(&fail).cell("t.add", Backend::Rvv).cell,
        Cell::Fail
    );

    // A pass cell that regresses is reported against the baseline.
    let baseline = inv.csv();
    assert!(regressions(&baseline, &inv).is_empty());
    let now = inventory(&rec("scalar", None, "pass", "test"));
    let lost = regressions(&baseline, &now);
    assert_eq!(lost, ["t.add on rvv: pass -> untested"]);
    assert!(inv.markdown().contains("| `t.add` |"));
}

#[test]
fn marker_files_need_an_entry_or_an_exemption() {
    let root = std::env::temp_dir().join(format!("accel-scan-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("src")).unwrap();
    for (file, text) in [
        ("src/add.rs", "use core::arch::x86_64::*;"),
        (
            "src/detect.rs",
            "std::arch::is_x86_feature_detected!(\"avx2\")",
        ),
        (
            "src/new.rs",
            "#[target_feature(enable = \"avx2\")] fn f() {}",
        ),
        ("src/plain.rs", "fn f() {}"),
    ] {
        std::fs::write(root.join(file), text).unwrap();
    }
    let missing = inventory("").unregistered_markers(&root).unwrap();
    assert_eq!(
        missing,
        ["src/new.rs: #[target_feature] without a registry entry"]
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn vector_instructions_are_recognized_per_isa() {
    let rvv = "vsetvli\ta0, a1, e8, m1, ta, ma\nvle8.v\tv8, (a2)\nadd\ta0, a0, a1\n";
    assert_eq!(vector_instructions(Backend::Rvv, rvv), 2);
    let neon64 = "ld4\t{ v0.16b, v1.16b, v2.16b, v3.16b }, [x0]\nadd\tx0, x0, #1\n";
    assert_eq!(vector_instructions(Backend::Neon64, neon64), 1);
    let neon32 = "vld4.8\t{d16, d17, d18, d19}, [r0]\nvmull.u8\tq8, d16, d20\nadd\tr0, r0, #1\n";
    assert_eq!(vector_instructions(Backend::Neon32, neon32), 2);
    let x86 = "paddb\t%xmm1, %xmm0\nvpaddb\t%ymm1, %ymm0, %ymm0\naddq\t$1, %rax\n";
    assert_eq!(vector_instructions(Backend::X86Sse2, x86), 1);
    assert_eq!(vector_instructions(Backend::X86Avx2, x86), 1);
    let listing = "0000000000001000 <k::add_rvv>:\n    1000:\tvsetvli\ta0, a1, e8, m1, ta, ma\n    1004:\tret\n\n0000000000002000 <other>:\n    2000:\tret\n";
    let funcs = functions(listing);
    assert_eq!(funcs.len(), 2);
    assert_eq!(funcs[0].0, "k::add_rvv");
    assert_eq!(vector_instructions(Backend::Rvv, &funcs[0].1), 1);
}
