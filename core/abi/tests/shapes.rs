//! A build made against an older Daedalus fits a newer one when what it has of every growable
//! shape is exactly the start of what the newer one has, and every frozen shape is the same.

use daedalus_abi::{
    FieldInfo, Grown, Mismatch, SHAPES, ShapeDesc, ShapeInfo, find, fits, read_grown,
};

const fn field(name: &'static str, offset: u32, size: u32) -> FieldInfo {
    FieldInfo { name, offset, size }
}

/// A growable shape as four builds have it: first two fields, then a third, then a fourth, and
/// one where the second and third changed places.
const V1: &[FieldInfo] = &[field("a", 0, 8), field("b", 8, 4)];
const V2: &[FieldInfo] = &[field("a", 0, 8), field("b", 8, 4), field("c", 12, 4)];
const V3: &[FieldInfo] = &[
    field("a", 0, 8),
    field("b", 8, 4),
    field("c", 12, 4),
    field("d", 16, 8),
];
const SWAPPED: &[FieldInfo] = &[field("a", 0, 8), field("c", 8, 4), field("b", 12, 4)];

fn table(fields: &'static [FieldInfo], size: u32) -> ShapeInfo {
    ShapeInfo {
        id: 1,
        name: "Table",
        frozen: false,
        size,
        align: 8,
        required: 2,
        fields,
    }
}

fn cell(size: u32, fields: &'static [FieldInfo]) -> ShapeInfo {
    ShapeInfo {
        id: 2,
        name: "Cell",
        frozen: true,
        size,
        align: 8,
        required: fields.len() as u16,
        fields,
    }
}

const CELL: &[FieldInfo] = &[field("x", 0, 8), field("y", 8, 8)];
const CELL_SWAPPED: &[FieldInfo] = &[field("y", 0, 8), field("x", 8, 8)];
const CELL_GROWN: &[FieldInfo] = &[field("x", 0, 8), field("y", 8, 8), field("z", 16, 8)];

fn build(shapes: &[ShapeInfo]) -> Vec<ShapeDesc> {
    shapes.iter().map(ShapeInfo::desc).collect()
}

#[test]
fn this_build_fits_itself() {
    assert_eq!(fits(SHAPES, daedalus_abi::DESCRIPTORS.as_slice()), Ok(()));
}

#[test]
fn an_older_build_fits_every_newer_one() {
    let host = [table(V3, 24), cell(16, CELL)];
    for older in [table(V1, 16), table(V2, 16), table(V3, 24)] {
        assert_eq!(fits(&host, &build(&[older, cell(16, CELL)])), Ok(()));
    }
    // A build from before a shape existed doesn't mention it, and fits.
    assert_eq!(fits(&host, &build(&[table(V1, 16)])), Ok(()));
}

#[test]
fn growth_into_what_was_padding_is_still_growth() {
    // V1 is 16 bytes with 4 of padding. V2 puts a field there and is no larger.
    let older = table(V1, 16).desc();
    assert_eq!((older.size, older.extent), (16, 12));
    assert_eq!(fits(&[table(V2, 16)], &[older]), Ok(()));
}

#[test]
fn a_newer_build_is_refused_and_the_shape_and_both_counts_are_named() {
    let host = [table(V2, 16)];
    let why = fits(&host, &build(&[table(V3, 24)])).expect_err("newer");
    assert_eq!(
        why,
        Mismatch::Newer {
            shape: "Table",
            here: 3,
            there: 4
        }
    );
    assert_eq!(
        why.to_string(),
        "needs a newer host: Table has 3 fields here, 4 there"
    );
    // So is one that uses a shape this host doesn't have at all.
    let why = fits(&host, &build(&[table(V2, 16), cell(16, CELL)])).expect_err("unknown");
    assert_eq!(why, Mismatch::Unknown { id: 2 });
}

#[test]
fn a_reordered_growable_shape_is_refused_by_name() {
    let why = fits(&[table(V3, 24)], &build(&[table(SWAPPED, 16)])).expect_err("moved");
    assert_eq!(
        why,
        Mismatch::Moved {
            shape: "Table",
            fields: 3
        }
    );
    assert_eq!(
        why.to_string(),
        "was built for a different Daedalus: the first 3 fields of Table are laid out \
         differently there"
    );
}

#[test]
fn a_changed_frozen_shape_is_refused_by_name() {
    let host = [cell(16, CELL)];
    // The same size, laid out another way.
    let why = fits(&host, &build(&[cell(16, CELL_SWAPPED)])).expect_err("reordered");
    assert_eq!(
        why.to_string(),
        "was built for a different Daedalus: Cell is laid out differently there"
    );
    // A frozen shape doesn't grow: one more field is a change, older or newer.
    let why = fits(&host, &build(&[cell(24, CELL_GROWN)])).expect_err("grown");
    assert_eq!(
        why.to_string(),
        "was built for a different Daedalus: Cell is 16 bytes here, 24 there"
    );
    let why = fits(&[cell(24, CELL_GROWN)], &build(&host)).expect_err("shrunk");
    assert!(matches!(why, Mismatch::Frozen { shape: "Cell", .. }));
}

#[test]
fn a_build_without_a_field_every_build_has_is_refused() {
    const ONE: &[FieldInfo] = &[field("a", 0, 8)];
    let why = fits(&[table(V2, 16)], &build(&[table(ONE, 8)])).expect_err("short");
    assert_eq!(
        why,
        Mismatch::Short {
            shape: "Table",
            required: 2,
            there: 1
        }
    );
}

/// A table element as an older build lays it out, and as this one does.
#[repr(C)]
#[derive(Clone, Copy)]
struct Old {
    a: u64,
    b: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct New {
    a: u64,
    b: u32,
    c: u32,
    d: Option<extern "C" fn()>,
}

// SAFETY: `c` and `d` are the fields added since, and zero is a valid value of each: no count,
// and no function.
unsafe impl Grown for New {
    const ID: u32 = 1;
}

#[test]
fn a_table_is_read_at_its_makers_stride_and_what_it_lacks_reads_as_absent() {
    // Garbage where the older build's padding is: it is nobody's, and must not be read as `c`.
    let mut raw = [0xAAu8; 48];
    for (index, (a, b)) in [(1u64, 10u32), (2, 20), (3, 30)].into_iter().enumerate() {
        raw[index * 16..index * 16 + 8].copy_from_slice(&a.to_ne_bytes());
        raw[index * 16 + 8..index * 16 + 12].copy_from_slice(&b.to_ne_bytes());
    }
    assert_eq!(size_of::<Old>(), 16);
    let theirs = build(&[table(V1, 16)]);
    let desc = find(&theirs, New::ID).expect("described");
    for (index, (a, b)) in [(1u64, 10u32), (2, 20), (3, 30)].into_iter().enumerate() {
        // SAFETY: `raw` is three elements of 16 bytes, as `desc` says, and it fits `V3`.
        let got = unsafe { read_grown(raw.as_ptr().cast::<New>(), index, desc) };
        assert_eq!((got.a, got.b, got.c), (a, b, 0));
        assert!(got.d.is_none(), "a function it never had is absent");
    }
}
