//! What one side of a boundary tells the other about its shapes, and the check that a build
//! made against an older Daedalus still fits a newer one.

use core::fmt;

use crate::layout::{FNV_START, mix};

/// One field of a shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FieldInfo {
    pub name: &'static str,
    pub offset: u32,
    pub size: u32,
}

/// One shape as this build has it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShapeInfo {
    pub id: u32,
    pub name: &'static str,
    /// Whether the shape can never change. A shape that isn't frozen grows at its end.
    pub frozen: bool,
    pub size: u32,
    pub align: u32,
    /// The fields every build has: all of a frozen shape's, and a growable shape's first ones.
    pub required: u16,
    /// The fields in the order they were added, which is the order they are declared in.
    pub fields: &'static [FieldInfo],
}

impl ShapeInfo {
    /// Where the first `fields` fields end: the bytes a build with only those fields touches.
    pub const fn extent_of(&self, fields: usize) -> u32 {
        let mut end = 0;
        let mut at = 0;
        while at < fields && at < self.fields.len() {
            let field = &self.fields[at];
            if field.offset + field.size > end {
                end = field.offset + field.size;
            }
            at += 1;
        }
        end
    }

    /// The hash of the first `fields` fields: the shape's id and alignment, and each field's
    /// name, offset and size. A frozen shape's size is in it too.
    pub const fn hash_of(&self, fields: usize) -> u64 {
        let mut hash = mix(mix(FNV_START, self.id as u64), self.align as u64);
        if self.frozen {
            hash = mix(hash, self.size as u64);
        }
        let mut at = 0;
        while at < fields && at < self.fields.len() {
            let field = &self.fields[at];
            // The name is what the field means: two of one size that change places are a
            // different layout, though every offset and size is as before.
            let name = field.name.as_bytes();
            let mut byte = 0;
            while byte < name.len() {
                hash = mix(hash, name[byte] as u64);
                byte += 1;
            }
            hash = mix(hash, field.offset as u64);
            hash = mix(hash, field.size as u64);
            at += 1;
        }
        hash
    }

    /// This shape as the other side reads it.
    pub const fn desc(&self) -> ShapeDesc {
        let fields = self.fields.len();
        ShapeDesc {
            id: self.id,
            size: self.size,
            extent: self.extent_of(fields),
            fields: fields as u16,
            needs: fields as u16,
            hash: self.hash_of(fields),
        }
    }
}

/// One shape as a build describes it to the other side. This struct never changes.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShapeDesc {
    pub id: u32,
    /// The shape's size in that build, which is the stride of a table of them.
    pub size: u32,
    /// Where its last field ends. Padding after it is nobody's.
    pub extent: u32,
    /// How many fields it has in that build.
    pub fields: u16,
    /// How many of them the build can't do without. Today a build needs all it has, and a host
    /// compares `fields`. It is here so that a build can one day say it uses fewer, and run
    /// against a host that has only those.
    pub needs: u16,
    /// The hash of those fields, as [`ShapeInfo::hash_of`] makes it.
    pub hash: u64,
}

impl ShapeDesc {
    pub const NONE: ShapeDesc = ShapeDesc {
        id: 0,
        size: 0,
        extent: 0,
        fields: 0,
        needs: 0,
        hash: 0,
    };
}

/// A build's descriptors: `count` of them at `shapes`. This struct never changes.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct AbiDesc {
    pub shapes: *const ShapeDesc,
    pub count: u32,
    pub reserved: u32,
}

/// Why a build made apart doesn't fit this one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mismatch {
    /// It has more of a growable shape than this build: it was made against a newer Daedalus.
    Newer {
        shape: &'static str,
        here: u16,
        there: u16,
    },
    /// It has a shape this build doesn't know at all: also newer.
    Unknown { id: u32 },
    /// A frozen shape is another size or laid out differently.
    Frozen {
        shape: &'static str,
        here: u32,
        there: u32,
    },
    /// The fields of a growable shape that both have are not laid out the same.
    Moved { shape: &'static str, fields: u16 },
    /// It lacks fields of a growable shape that every build has.
    Short {
        shape: &'static str,
        required: u16,
        there: u16,
    },
}

/// Reads on after the name of what doesn't fit: "this plugin needs a newer host: ...".
impl fmt::Display for Mismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Mismatch::Newer { shape, here, there } => write!(
                f,
                "needs a newer host: {shape} has {here} fields here, {there} there"
            ),
            Mismatch::Unknown { id } => write!(
                f,
                "needs a newer host: it uses a shape (id {id}) this host doesn't have"
            ),
            Mismatch::Frozen { shape, here, there } if here != there => write!(
                f,
                "was built for a different Daedalus: {shape} is {here} bytes here, {there} there"
            ),
            Mismatch::Frozen { shape, .. } => write!(
                f,
                "was built for a different Daedalus: {shape} is laid out differently there"
            ),
            Mismatch::Moved { shape, fields } => write!(
                f,
                "was built for a different Daedalus: the first {fields} fields of {shape} \
                 are laid out differently there"
            ),
            Mismatch::Short {
                shape,
                required,
                there,
            } => write!(
                f,
                "was built for a different Daedalus: {shape} has {there} fields there, and \
                 every build has at least {required}"
            ),
        }
    }
}

/// Whether a build that describes itself with `theirs` fits one whose shapes are `ours`: every
/// frozen shape is the same, and what it has of every growable shape is exactly the start of
/// what this build has. A shape of ours it doesn't mention came after it was built, and it can't
/// reach one.
pub fn fits(ours: &[ShapeInfo], theirs: &[ShapeDesc]) -> Result<(), Mismatch> {
    for their in theirs {
        let Some(our) = ours.iter().find(|shape| shape.id == their.id) else {
            return Err(Mismatch::Unknown { id: their.id });
        };
        let here = our.fields.len() as u16;
        if our.frozen {
            if their.size != our.size || their.fields != here || their.hash != our.desc().hash {
                return Err(Mismatch::Frozen {
                    shape: our.name,
                    here: our.size,
                    there: their.size,
                });
            }
            continue;
        }
        if their.fields > here {
            return Err(Mismatch::Newer {
                shape: our.name,
                here,
                there: their.fields,
            });
        }
        if their.fields < our.required {
            return Err(Mismatch::Short {
                shape: our.name,
                required: our.required,
                there: their.fields,
            });
        }
        let shared = their.fields as usize;
        if their.extent != our.extent_of(shared) || their.hash != our.hash_of(shared) {
            return Err(Mismatch::Moved {
                shape: our.name,
                fields: their.fields,
            });
        }
    }
    Ok(())
}

/// The descriptor of shape `id` among `theirs`.
pub fn find(theirs: &[ShapeDesc], id: u32) -> Option<&ShapeDesc> {
    theirs.iter().find(|shape| shape.id == id)
}

/// A shape read from a table its maker laid out with fewer fields than this build has, or the
/// same.
///
/// # Safety
/// Every bit pattern that is this build's required fields followed by zeroes must be a valid
/// value: a field added to a growable shape means "absent" when it is zero.
pub unsafe trait Grown: Copy {
    /// The shape's id in [`SHAPES`](crate::SHAPES).
    const ID: u32;
}

/// Element `index` of a table of `T` laid out by a build that describes `T` as `theirs`: the
/// bytes it has, and zeroes for the fields it doesn't. What is copied is the start of a `T` with
/// all its required fields, because `theirs` has passed [`fits`] against this build.
///
/// # Safety
/// `table` must hold at least `index + 1` elements of `theirs.size` bytes each, and `theirs`
/// must have passed [`fits`] against this build's shapes.
pub unsafe fn read_grown<T: Grown>(table: *const T, index: usize, theirs: &ShapeDesc) -> T {
    let mut value = core::mem::MaybeUninit::<T>::zeroed();
    let have = (theirs.extent as usize).min(core::mem::size_of::<T>());
    // SAFETY: the element is `theirs.size` bytes at that stride, of which the first
    // `theirs.extent` are fields, as the caller says. `value` is a `T`, at least `have` bytes.
    unsafe {
        let from = table.cast::<u8>().add(index * theirs.size as usize);
        core::ptr::copy_nonoverlapping(from, value.as_mut_ptr().cast::<u8>(), have);
        // SAFETY: the required fields were copied and the rest is zero, which `Grown` says is
        // a valid `T`.
        value.assume_init()
    }
}
