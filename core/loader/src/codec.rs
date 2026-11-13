//! Codecs a plugin declares for its plain-data types. A codec converts a type's bytes to and from
//! the host value form and the wire form. Each of its four conversions joins the registry as a
//! conversion node. The codec holds its generation, so the library stays open while the codec does.

use std::path::Path;
use std::ptr;
use std::sync::Arc;

use daedalus_abi::{ByteCodecFn, CodecC, Status};
use daedalus_registry::{Conversion, OwnedBody, OwnedDecl, OwnedPort, TypeRef};
use daedalus_types::TypeKey;

use crate::generation::Generation;
use crate::open::LoadError;
use crate::pod::{PodBlock, PodError};
use crate::records::copy_text;

/// Which of a codec's four conversions to run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    /// The plugin's bytes to the host value form.
    ToValue,
    /// The host value form to the plugin's bytes.
    FromValue,
    /// The plugin's bytes to the wire form.
    ToBytes,
    /// The wire form to the plugin's bytes.
    FromBytes,
}

/// A codec a plugin declared, loaded. It holds the generation its functions belong to, so a clone
/// of it keeps the library open as well.
#[derive(Clone)]
pub struct Codec {
    key: String,
    value_key: String,
    wire_key: String,
    layout: u64,
    to_value: ByteCodecFn,
    from_value: ByteCodecFn,
    to_bytes: ByteCodecFn,
    from_bytes: ByteCodecFn,
    generation: Arc<Generation>,
}

impl Codec {
    /// The plugin's type key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// The host value type the value conversions name.
    pub fn value_key(&self) -> &str {
        &self.value_key
    }

    /// The wire type the wire conversions name.
    pub fn wire_key(&self) -> &str {
        &self.wire_key
    }

    /// The layout hash of the plugin's type.
    pub fn layout(&self) -> u64 {
        self.layout
    }

    /// The generation this codec's functions belong to. The codec keeps it open.
    pub fn generation(&self) -> &Generation {
        &self.generation
    }

    /// Runs one conversion over `input`, and returns its output. A plugin that refuses the input
    /// gives a typed error with the codec's key.
    pub fn convert(&self, direction: Direction, input: &[u8]) -> Result<Vec<u8>, LoadError> {
        let convert = match direction {
            Direction::ToValue => self.to_value,
            Direction::FromValue => self.from_value,
            Direction::ToBytes => self.to_bytes,
            Direction::FromBytes => self.from_bytes,
        };
        let mut needed = 0usize;
        // SAFETY: `convert` belongs to the library this codec's generation holds. The input is
        // valid for its length for the call, and a null output asks only for the size.
        let status =
            unsafe { convert(input.as_ptr(), input.len(), ptr::null_mut(), 0, &mut needed) };
        if status != Status::OK {
            return Err(self.refused(status));
        }
        let mut out = vec![0u8; needed];
        // SAFETY: as above, and `out` holds `needed` writable bytes, the size the plugin asked for.
        let status = unsafe {
            convert(
                input.as_ptr(),
                input.len(),
                out.as_mut_ptr(),
                out.len(),
                &mut needed,
            )
        };
        if status != Status::OK {
            return Err(self.refused(status));
        }
        out.truncate(needed);
        Ok(out)
    }

    /// The plugin's bytes as a block, named by this codec's key and layout.
    pub fn block(&self, bytes: &[u8]) -> PodBlock {
        PodBlock::new(&self.key, self.layout, bytes)
    }

    /// The bytes of a block, if the block has this codec's key and layout.
    pub fn accept<'a>(&self, block: &'a PodBlock) -> Result<&'a [u8], PodError> {
        block.accept(&self.key, self.layout)
    }

    fn refused(&self, status: Status) -> LoadError {
        LoadError::Codec {
            key: self.key.clone(),
            status,
        }
    }
}

/// Reads one codec of an entry. The codec and its four conversion declarations are returned.
///
/// # Safety
/// `codec` must come from an entry whose library `generation` holds, and stay loaded during the
/// call.
pub(crate) unsafe fn read_codec(
    codec: &CodecC,
    path: &Path,
    generation: &Arc<Generation>,
) -> Result<(Codec, [OwnedDecl; 4]), LoadError> {
    // SAFETY: the codec's text belongs to the library `generation` holds, as this function requires.
    let key = unsafe { copy_text(codec.key, path, "codec key")? };
    // SAFETY: as above.
    let value_key = unsafe { copy_text(codec.value_key, path, "codec value key")? };
    // SAFETY: as above.
    let wire_key = unsafe { copy_text(codec.wire_key, path, "codec wire key")? };
    // SAFETY: as above.
    let backend = unsafe { copy_text(codec.backend, path, "codec backend")? };
    let meta = Conversion {
        cost: codec.cost,
        lossy: codec.lossy,
    };
    let decls = [
        conversion(
            format!("{key}.to_value"),
            &key,
            &value_key,
            &backend,
            "to_value",
            meta,
            path,
        )?,
        conversion(
            format!("{key}.from_value"),
            &value_key,
            &key,
            &backend,
            "from_value",
            meta,
            path,
        )?,
        conversion(
            format!("{key}.to_bytes"),
            &key,
            &wire_key,
            &backend,
            "to_bytes",
            meta,
            path,
        )?,
        conversion(
            format!("{key}.from_bytes"),
            &wire_key,
            &key,
            &backend,
            "from_bytes",
            meta,
            path,
        )?,
    ];
    let loaded = Codec {
        key,
        value_key,
        wire_key,
        layout: codec.layout,
        to_value: codec.to_value,
        from_value: codec.from_value,
        to_bytes: codec.to_bytes,
        from_bytes: codec.from_bytes,
        generation: generation.clone(),
    };
    Ok((loaded, decls))
}

/// Reads every codec of an entry, and the conversion declarations they make.
///
/// # Safety
/// `entry` must come from a library `generation` holds, and stay loaded during the call.
pub(crate) unsafe fn read_codecs(
    entry: &daedalus_abi::PluginEntry,
    shapes: &[daedalus_abi::ShapeDesc],
    path: &Path,
    generation: &Arc<Generation>,
) -> Result<(Vec<Codec>, Vec<OwnedDecl>), LoadError> {
    // SAFETY: the entry declares `codec_count` codecs at `codecs`, valid while the library is
    // held, and `shapes` are that plugin's, as the caller says.
    let declared = unsafe {
        crate::open::read_table(entry.codecs, entry.codec_count, shapes, path, "codec list")?
    };
    let mut codecs = Vec::with_capacity(declared.len());
    let mut conversions = Vec::new();
    for codec in &declared {
        // SAFETY: each codec comes from the entry this function was given.
        let (codec, decls) = unsafe { read_codec(codec, path, generation)? };
        codecs.push(codec);
        conversions.extend(decls);
    }
    Ok((codecs, conversions))
}

/// One conversion node: a one-in, one-out declaration that is a conversion between two types.
fn conversion(
    id: String,
    input: &str,
    output: &str,
    backend: &str,
    text: &str,
    meta: Conversion,
    path: &Path,
) -> Result<OwnedDecl, LoadError> {
    Ok(OwnedDecl {
        id,
        source: path.display().to_string(),
        inputs: vec![OwnedPort {
            name: "in".to_owned(),
            ty: TypeRef::Key(type_key(input, path)?),
            optional: false,
        }],
        outputs: vec![OwnedPort {
            name: "out".to_owned(),
            ty: TypeRef::Key(type_key(output, path)?),
            optional: false,
        }],
        body: OwnedBody {
            backend: backend.to_owned(),
            text: text.to_owned(),
            code: None,
        },
        conversion: Some(meta),
    })
}

fn type_key(text: &str, path: &Path) -> Result<TypeKey, LoadError> {
    TypeKey::shared(text).map_err(|_| LoadError::BadText {
        path: path.to_path_buf(),
        what: "codec type key",
    })
}
