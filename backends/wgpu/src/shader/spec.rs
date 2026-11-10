//! What the authoring macro knows about a shader, as a constant. Nothing here is parsed while
//! running.

use crate::texture::Format;

/// How a storage texture is used.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TextureAccess {
    Read,
    Write,
    ReadWrite,
}

/// What a sampled texture gives a shader.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SampleKind {
    Float,
    Uint,
    Sint,
    Depth,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BindingKind {
    /// A storage buffer. `stride` is the size of one element of its array, or 0 when it has none.
    Storage {
        read_only: bool,
        stride: u32,
    },
    /// A uniform buffer of `size` bytes.
    Uniform {
        size: u32,
    },
    /// A two-dimensional texture that is sampled or loaded.
    Texture {
        sample: SampleKind,
    },
    /// A two-dimensional texture a compute pass reads or writes directly.
    StorageTexture {
        format: Format,
        access: TextureAccess,
    },
    Sampler {
        comparison: bool,
    },
}

impl BindingKind {
    /// Whether the shader writes through this binding.
    pub const fn writes(&self) -> bool {
        match self {
            BindingKind::Storage { read_only, .. } => !*read_only,
            BindingKind::StorageTexture { access, .. } => !matches!(access, TextureAccess::Read),
            _ => false,
        }
    }

    const fn tag(&self) -> [u32; 3] {
        match *self {
            BindingKind::Storage { read_only, stride } => [1, read_only as u32, stride],
            BindingKind::Uniform { size } => [2, size, 0],
            BindingKind::Texture { sample } => [3, sample as u32, 0],
            BindingKind::StorageTexture { format, access } => [4, format as u32, access as u32],
            BindingKind::Sampler { comparison } => [5, comparison as u32, 0],
        }
    }
}

/// One `@group(0) @binding(n)` of a shader. `name` is the WGSL variable's, and is the port's
/// name when a port feeds it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BindingSpec {
    pub index: u32,
    pub name: &'static str,
    pub kind: BindingKind,
}

/// How many workgroups one run dispatches.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Dispatch {
    /// Exactly these.
    Counts([u32; 3]),
    /// One invocation per element of the storage buffer at `binding`, along x.
    PerElement { binding: u32, stride: u32 },
}

/// A whole compute shader: one pass, its bindings and its shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ShaderSpec {
    pub name: &'static str,
    pub entry: &'static str,
    pub source: &'static str,
    pub workgroup: [u32; 3],
    /// In binding order.
    pub bindings: &'static [BindingSpec],
    pub dispatch: Dispatch,
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

const fn fnv(mut hash: u64, bytes: &[u8]) -> u64 {
    let mut at = 0;
    while at < bytes.len() {
        hash = (hash ^ bytes[at] as u64).wrapping_mul(FNV_PRIME);
        at += 1;
    }
    // A separator, so "ab" + "c" and "a" + "bc" differ.
    (hash ^ 0xff).wrapping_mul(FNV_PRIME)
}

impl ShaderSpec {
    /// The pipeline cache key: FNV-1a over the name, entry, source and binding table. It depends
    /// on content only, so it is the same in every process and every Rust release, and two
    /// identical specs share a pipeline.
    pub const fn key(&self) -> u64 {
        let mut hash = fnv(FNV_OFFSET, self.name.as_bytes());
        hash = fnv(hash, self.entry.as_bytes());
        hash = fnv(hash, self.source.as_bytes());
        let mut at = 0;
        while at < self.bindings.len() {
            let binding = &self.bindings[at];
            let tag = binding.kind.tag();
            hash = fnv(hash, &binding.index.to_le_bytes());
            hash = fnv(hash, binding.name.as_bytes());
            hash = fnv(hash, &tag[0].to_le_bytes());
            hash = fnv(hash, &tag[1].to_le_bytes());
            hash = fnv(hash, &tag[2].to_le_bytes());
            at += 1;
        }
        hash
    }

    /// How many bindings of each limited kind the shader has: storage buffers, storage textures.
    pub const fn counts(&self) -> (u32, u32) {
        let (mut buffers, mut textures, mut at) = (0, 0, 0);
        while at < self.bindings.len() {
            match self.bindings[at].kind {
                BindingKind::Storage { .. } => buffers += 1,
                BindingKind::StorageTexture { .. } => textures += 1,
                _ => {}
            }
            at += 1;
        }
        (buffers, textures)
    }
}
