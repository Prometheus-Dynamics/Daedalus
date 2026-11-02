use super::*;
use crate::{Backend, Exempt, Ext, Kernel, Op, Registry, Status};

static OPS: [Op; 2] = [
    Op::new("t.add", "add", "t::add").could_use(&[(Backend::Neon64, &[Ext::Dotprod])]),
    Op::new("t.hash", "hash", "t::hash").not_per_element(),
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
        Kernel::implemented("t.add", Backend::X86Avx2, "src/add.rs:30", "add::avx2")
            .with_extensions(&[Ext::Bmi2]),
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
    // Extension columns: a declared candidate without a kernel is a gap; another op's
    // extension column does not apply; a base pass does not count for the variant.
    let dotprod = Column {
        backend: Backend::Neon64,
        extensions: vec!["dotprod".into()],
        core: None,
        wired: false,
    };
    assert_eq!(inv.cell_in("t.add", &dotprod).cell, Cell::Gap);
    assert_eq!(inv.cell_in("t.hash", &dotprod).cell, Cell::NotApplicable);
    let bmi2 = Column {
        backend: Backend::X86Avx2,
        extensions: vec!["bmi2".into()],
        core: None,
        wired: false,
    };
    assert_eq!(inv.cell_in("t.add", &bmi2).cell, Cell::Untested);
    assert!(inv.columns().iter().any(|c| c.name() == "x86_avx2+bmi2"));
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
fn wired_needs_every_call_site_to_call_the_kernel() {
    static SITES: [crate::CallSite; 1] = [crate::CallSite::new("src/user.rs", "kernels::add(")
        .forbid(&["for i in 0..n { a[i] += b[i] }"])];
    static WIRED: [Op; 1] = [Op::new("t.add", "add", "t::add").wired_at(&SITES)];
    static REG: Registry = Registry {
        repo: "t",
        ops: &WIRED,
        ..Registry::EMPTY
    };
    let root = std::env::temp_dir().join(format!("accel-wired-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("src")).unwrap();
    let mut inv = Inventory::default();
    inv.add_registry(&crate::dump_json(&[&REG])).unwrap();
    assert_eq!(inv.wired_cell("t.add").cell, Cell::Untested, "no root yet");
    inv.root = Some(root.clone());
    assert_eq!(inv.wired_cell("t.add").cell, Cell::Gap, "file missing");
    std::fs::write(root.join("src/user.rs"), "fn f() { scalar() }").unwrap();
    assert_eq!(inv.wired_cell("t.add").cell, Cell::Gap, "kernel not called");
    std::fs::write(root.join("src/user.rs"), "fn f() { kernels::add(a, b) }").unwrap();
    assert_eq!(inv.wired_cell("t.add").cell, Cell::Pass);
    std::fs::write(
        root.join("src/user.rs"),
        "fn f() { kernels::add(a, b); for i in 0..n { a[i] += b[i] } }",
    )
    .unwrap();
    assert_eq!(
        inv.wired_cell("t.add").cell,
        Cell::Fail,
        "the old loop is still there"
    );
    let _ = std::fs::remove_dir_all(&root);
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
    assert_eq!(
        vector_instructions(Backend::Rvv, rvv),
        1,
        "vle8; vsetvli alone is not vector work"
    );
    let neon64 = "ld4\t{ v0.16b, v1.16b, v2.16b, v3.16b }, [x0]\nadd\tx0, x0, #1\n";
    assert_eq!(vector_instructions(Backend::Neon64, neon64), 1);
    let neon32 = "vld4.8\t{d16, d17, d18, d19}, [r0]\nvmull.u8\tq8, d16, d20\nadd\tr0, r0, #1\n";
    assert_eq!(vector_instructions(Backend::Neon32, neon32), 2);
    let x86 = "paddb\t%xmm1, %xmm0\nvpaddb\t%ymm1, %ymm0, %ymm0\naddq\t$1, %rax\nvmulss\t%xmm2, %xmm5, %xmm5\nvxorps\t%xmm0, %xmm0, %xmm0\nmulps\t%xmm1, %xmm2\n";
    assert_eq!(
        vector_instructions(Backend::X86Sse2, x86),
        2,
        "paddb and mulps; not mulss or zeroing"
    );
    assert_eq!(vector_instructions(Backend::X86Avx2, x86), 1);
    let vfp = "vmul.f64\td0, d1, d2\nvadd.f32\ts0, s1, s2\nvldr\td0, [r0]\nvadd.f32\tq0, q1, q2\n";
    assert_eq!(
        vector_instructions(Backend::Neon32, vfp),
        1,
        "only the q-register add"
    );
    let listing = "0000000000001000 <k::add_rvv>:\n    1000:\tvsetvli\ta0, a1, e8, m1, ta, ma\n    1004:\tret\n\n0000000000002000 <other>:\n    2000:\tret\n";
    let funcs = functions(listing);
    assert_eq!(funcs.len(), 2);
    assert_eq!(funcs[0].0, "k::add_rvv");
    assert_eq!(
        vector_instructions(Backend::Rvv, &funcs[0].1),
        0,
        "vsetvli only"
    );
}

static CORE_ROOT: Registry = Registry {
    repo: "t",
    name: "core",
    ops: &[Op::new("t.sum", "sum", "t::sum")],
    kernels: &[
        Kernel::implemented("t.sum", Backend::Neon64, "src/sum.rs:1", "sum::neon"),
        Kernel::implemented("t.sum", Backend::Neon64, "src/sum.rs:9", "sum::a53").with_core("a53"),
        Kernel::implemented("t.sum", Backend::Neon64, "src/sum.rs:20", "sum::a55")
            .with_core("a55")
            .with_extensions(&[Ext::Dotprod]),
    ],
    ..Registry::EMPTY
};

/// A core-tuned variant (`with_core`) is its own column, passing only from its own evidence;
/// the shared column ignores it.
#[test]
fn core_tuned_variants_get_their_own_columns() {
    let record = |variant: &str, exts: &str| {
        format!(
            "{{\"kind\":\"test\",\"op\":\"t.sum\",\"backend\":\"neon64\",\"extensions\":[{exts}],\"variant\":{variant},\"adapter\":null,\"target\":\"x\",\"runner\":\"qemu\",\"vlen\":null,\"rev\":\"\",\"result\":\"pass\",\"detail\":null}}\n"
        )
    };
    let mut inv = Inventory::default();
    inv.add_registry(&crate::dump_json(&[&CORE_ROOT])).unwrap();
    inv.add_records(&(record("null", "") + &record("\"a55\"", "\"dotprod\"")))
        .unwrap();
    let names: Vec<String> = inv.columns().iter().map(Column::name).collect();
    let at = |name: &str| names.iter().position(|n| n == name).unwrap_or(usize::MAX);
    assert!(at("neon64") < at("neon64[a53]"), "{names:?}");
    assert!(
        at("neon64+dotprod") < at("neon64+dotprod[a55]"),
        "{names:?}"
    );
    let column = |name: &str| {
        inv.columns()
            .into_iter()
            .find(|c| c.name() == name)
            .unwrap()
    };
    // The shared column passes from the base kernel alone; a53 has no evidence of its own.
    assert_eq!(inv.cell("t.sum", Backend::Neon64).cell, Cell::Pass);
    assert_eq!(
        inv.cell_in("t.sum", &column("neon64[a53]")).cell,
        Cell::Untested
    );
    assert_eq!(
        inv.cell_in("t.sum", &column("neon64+dotprod[a55]")).cell,
        Cell::Pass
    );
    // No shared dotprod kernel: that column does not apply.
    assert_eq!(
        inv.cell_in("t.sum", &column("neon64+dotprod")).cell,
        Cell::NotApplicable
    );
}

static EXT_OPS: [Op; 1] = [Op::new("t.mul", "mul", "t::mul").could_use(&[
    (Backend::Neon64, &[Ext::Dotprod]),
    (Backend::Neon64, &[Ext::I8mm]),
])];

static EXT_ROOT: Registry = Registry {
    repo: "t",
    name: "ext",
    ops: &EXT_OPS,
    kernels: &[
        Kernel::implemented("t.mul", Backend::Scalar, "src/mul.rs:1", "mul::scalar"),
        Kernel::missing(
            "t.mul",
            Backend::Neon64,
            Status::Impossible("no widening form for this width"),
        )
        .with_extensions(&[Ext::Dotprod]),
        Kernel::missing(
            "t.mul",
            Backend::Gpu,
            Status::Deferred("waiting on the SDK"),
        ),
    ],
    ..Registry::EMPTY
};

#[test]
fn extension_candidates_can_be_impossible_and_cells_deferred() {
    let mut inv = Inventory::default();
    inv.add_registry(&crate::dump_json(&[&EXT_ROOT])).unwrap();
    assert!(
        inv.check_registry().is_empty(),
        "{:?}",
        inv.check_registry()
    );
    let column = |name: &str| {
        inv.columns()
            .into_iter()
            .find(|c| c.name() == name)
            .unwrap()
    };
    let dotprod = inv.cell_in("t.mul", &column("neon64+dotprod"));
    assert_eq!(dotprod.cell, Cell::Impossible);
    assert_eq!(dotprod.reason, "no widening form for this width");
    assert_eq!(inv.cell_in("t.mul", &column("neon64+i8mm")).cell, Cell::Gap);
    let gpu = inv.cell("t.mul", Backend::Gpu);
    assert_eq!(gpu.cell, Cell::Deferred);
    assert_eq!(gpu.reason, "waiting on the SDK");
    let md = inv.markdown();
    assert!(md.contains("deferred 1") && md.contains("gap "), "{md}");
    assert!(
        inv.csv()
            .contains("t,t.mul,gpu,deferred,waiting on the SDK,")
    );
}

static NODE_OPS: [Op; 1] = [Op::new("t.shrink", "shrink", "t::shrink").only_on(
    &[Backend::Scalar, Backend::FixedFunction],
    "a fixed-function node; the CPU kernels and callers are another repo's",
)];

static NODE_ROOT: Registry = Registry {
    repo: "t",
    name: "node",
    ops: &NODE_OPS,
    kernels: &[
        Kernel::implemented("t.shrink", Backend::Scalar, "src/s.rs:1", "s::scalar"),
        Kernel::implemented("t.shrink", Backend::FixedFunction, "src/s.rs:9", "s::rga")
            .with_variant("rga"),
    ],
    ..Registry::EMPTY
};

#[test]
fn only_on_ops_have_no_other_columns() {
    let mut inv = Inventory::default();
    inv.add_registry(&crate::dump_json(&[&NODE_ROOT])).unwrap();
    assert!(
        inv.check_registry().is_empty(),
        "{:?}",
        inv.check_registry()
    );
    assert_eq!(
        inv.cell("t.shrink", Backend::Neon64).cell,
        Cell::NotApplicable
    );
    assert_eq!(inv.cell("t.shrink", Backend::Gpu).cell, Cell::NotApplicable);
    assert_eq!(
        inv.cell_in("t.shrink", &Column::wired()).cell,
        Cell::NotApplicable
    );
    assert_eq!(inv.cell("t.shrink", Backend::Scalar).cell, Cell::Untested);
    assert_eq!(
        inv.cell("t.shrink", Backend::FixedFunction).cell,
        Cell::Untested
    );
    assert!(
        inv.markdown()
            .contains("`t.shrink` only on scalar, fixed_function: a fixed-function node")
    );

    // An entry on an excluded backend, or no reason, is a registry error.
    static BAD_OPS: [Op; 1] = [Op::new("t.bad", "bad", "t::bad").only_on(&[Backend::Scalar], " ")];
    static BAD: Registry = Registry {
        repo: "t",
        name: "bad",
        ops: &BAD_OPS,
        kernels: &[Kernel::implemented(
            "t.bad",
            Backend::Rvv,
            "src/b.rs:1",
            "b::rvv",
        )],
        ..Registry::EMPTY
    };
    let mut inv = Inventory::default();
    inv.add_registry(&crate::dump_json(&[&BAD])).unwrap();
    let errors = inv.check_registry();
    assert!(
        errors.iter().any(|e| e.contains("needs a reason")),
        "{errors:?}"
    );
    assert!(
        errors.iter().any(|e| e.contains("only_on other backends")),
        "{errors:?}"
    );
}
