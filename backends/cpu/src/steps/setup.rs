//! What a body needs to be built from a plan. Only with an allocator.

use alloc::boxed::Box;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use core::mem::size_of;
use daedalus_backend::ConstOf;
use daedalus_runner::Held;

use daedalus_types::{FromLiteral, Literal, NodeIndex};

pub use alloc::string::String as StepString;
pub use daedalus_graph::Lit;

use super::live::{Arrived, Live, Readers};
use super::{ConstTable, Keep, Kind, LitOf, ports};
use daedalus_abi::Slot;

/// What one node in one graph needs from the plan.
pub struct Setup<'a> {
    pub node: NodeIndex,
    pub inputs: u16,
    pub consts: &'a [ConstOf<'a>],
    /// What each output of the node keeps between ticks, by output index. An output not listed
    /// is `Fresh`.
    pub keep: &'a [super::Keep],
    /// Which inputs read a value in `cpu.wire`, whose cells hold their type's wire form: a bit
    /// for each fixed input ([`ports::wire_bit`]), and [`ports::FAN_IN_WIRE`] for the fan-in.
    /// Each is read through its type's `WireForm`.
    pub wire_inputs: u32,
    /// The node's id, for a refusal to name.
    pub id: &'a str,
    /// Room lent for the node's outputs placed in memory another backend offers: see
    /// [`super::OutView`].
    pub lent: &'a [daedalus_backend::LentRoom],
    /// Where the step registers how its constants are decoded, so a host can change one while
    /// the graph runs. `None` fixes them as the step is built.
    pub readers: Option<&'a Readers>,
}

impl Setup<'_> {
    fn value(&self, port: &str) -> Option<&Lit> {
        self.consts
            .iter()
            .find(|one| one.node == self.node && one.port == port)
            .map(|one| one.value)
    }

    /// The config's fields as literals, from its `config` struct. The planner folds a field set
    /// by a const of its own into that struct, so this is the one form a plan gives.
    pub fn config_fields(&self) -> Vec<(String, Literal)> {
        self.value("config").and_then(fields_of).unwrap_or_default()
    }
}

/// A config struct's fields as literals. `None` when the value is no struct.
fn fields_of(value: &Lit) -> Option<Vec<(String, Literal)>> {
    let Lit::Struct(all) = value else {
        return None;
    };
    let fields = all.iter();
    Some(
        fields
            .map(|field| (field.name.clone(), literal(&field.value)))
            .collect(),
    )
}

/// Builds a node's config from the plan with `decode`, and registers `decode` for a value that
/// comes later: one decoder, so a patched config is refused in the words a planned one is.
pub fn read_config<C: Send + 'static>(
    setup: &Setup<'_>,
    consts: &mut Consts,
    decode: impl Fn(&[(String, Literal)]) -> Result<C, String> + Send + Sync + 'static,
) -> Result<C, String> {
    let config = decode(&setup.config_fields())?;
    consts
        .live
        .read(setup.readers, setup.node, "config", move |value| {
            let fields =
                fields_of(value).ok_or_else(|| String::from("the config doesn't fit its type"))?;
            let config: Box<dyn core::any::Any + Send> = Box::new(decode(&fields)?);
            Ok(Arrived::Config(config))
        });
    Ok(config)
}

/// Finds a field in [`Setup::config_fields`]' result.
pub fn field<'a>(fields: &'a [(String, Literal)], name: &str) -> Option<&'a Literal> {
    fields
        .iter()
        .find(|(field, _)| field == name)
        .map(|(_, value)| value)
}

/// How many bytes [`literal`] has kept for the life of the process.
#[cfg(target_has_atomic = "ptr")]
static KEPT: core::sync::atomic::AtomicUsize = core::sync::atomic::AtomicUsize::new(0);

#[inline]
fn kept(bytes: usize) {
    #[cfg(target_has_atomic = "ptr")]
    KEPT.fetch_add(bytes, core::sync::atomic::Ordering::Relaxed);
    #[cfg(not(target_has_atomic = "ptr"))]
    let _ = bytes;
}

/// The bytes decoding constants has kept for the life of this process so far. It only grows. Numbers and bools add nothing; each decode of a text, a list,
/// a struct or an enum adds its size, and a constant is decoded once when its step is built and
/// once for each patch that changes it.
pub fn literal_bytes_kept() -> usize {
    #[cfg(target_has_atomic = "ptr")]
    return KEPT.load(core::sync::atomic::Ordering::Relaxed);
    #[cfg(not(target_has_atomic = "ptr"))]
    0
}

/// A plan value as the `'static` form a config reads. Text, lists, structs and enum names are
/// kept for the life of the process, once per decode: [`literal_bytes_kept`] counts them.
fn literal(value: &Lit) -> Literal {
    match value {
        Lit::Unit => Literal::Unit,
        Lit::Enum { name, payload } => {
            kept(name.len() + payload.iter().len() * size_of::<Literal>());
            let name: &'static str = Box::leak(name.clone().into_boxed_str());
            let payload: Option<&'static Literal> = payload
                .as_deref()
                .map(|value| &*Box::leak(Box::new(literal(value))));
            Literal::Enum(name, payload)
        }
        Lit::Bool(value) => Literal::Bool(*value),
        Lit::Int(value) => Literal::Int(*value),
        Lit::Float(value) => Literal::Float(*value),
        Lit::Str(value) => {
            kept(value.len());
            Literal::Str(Box::leak(value.clone().into_boxed_str()))
        }
        Lit::Bytes(value) => {
            kept(value.len());
            Literal::Bytes(Box::leak(value.clone().into_boxed_slice()))
        }
        Lit::List(values) => {
            kept(values.len() * size_of::<Literal>());
            let all: Vec<Literal> = values.iter().map(literal).collect();
            Literal::List(Box::leak(all.into_boxed_slice()))
        }
        Lit::Struct(fields) => {
            let names: usize = fields.iter().map(|field| field.name.len()).sum();
            kept(names + fields.len() * size_of::<(&'static str, Literal)>());
            let all: Vec<(&'static str, Literal)> = fields
                .iter()
                .map(|field| {
                    let name: &'static str = Box::leak(field.name.clone().into_boxed_str());
                    (name, literal(&field.value))
                })
                .collect();
            Literal::Struct(Box::leak(all.into_boxed_slice()))
        }
    }
}

impl<T: FromLiteral + 'static> LitOf<T> {
    pub const DECODE: Option<fn(&Lit) -> Option<T>> =
        Some(|value| T::from_literal(&literal(value)).ok());
}

/// One constant per fixed input that has one, and where a later value for one arrives. Empty
/// when the node has none.
#[derive(Default)]
pub struct Consts {
    held: Vec<Option<Held>>,
    live: Live,
}

impl ConstTable for Consts {
    #[inline]
    fn slot(&self, at: usize) -> Option<&Slot> {
        self.held.get(at)?.as_ref().map(Held::slot)
    }

    #[cold]
    #[inline(never)]
    fn refresh<C: 'static>(&mut self, config: &mut C) {
        for arrived in self.live.take() {
            match arrived {
                Arrived::Input(at, held) => {
                    if let Some(slot) = self.held.get_mut(at) {
                        *slot = Some(held);
                    }
                }
                Arrived::Config(new) => {
                    if let Ok(new) = new.downcast::<C>() {
                        *config = *new;
                    }
                }
            }
        }
    }
}

/// Refuses an output placed in `cpu.wire` whose type has no wire form: `outputs` are each output's
/// name, whether its type has a wire form, and the type's name.
pub fn check_wire(setup: &Setup<'_>, outputs: &[(&str, bool, &str)]) -> Result<(), String> {
    for (at, (name, has, ty)) in outputs.iter().enumerate() {
        if setup.keep.get(at) == Some(&Keep::Wire) && !has {
            return Err(format!(
                "output {name} of {} goes to a worker, but {ty} has no wire form: add #[derive(WireForm)] to {ty}",
                setup.id
            ));
        }
    }
    Ok(())
}

/// The empty table for `fixed` inputs. It only takes room if this node has a constant.
pub fn consts_for(setup: &Setup<'_>, fixed: usize) -> Consts {
    let any = setup.consts.iter().any(|one| one.node == setup.node);
    let mut consts = Consts::default();
    if any {
        consts.held.resize_with(fixed, || None);
    }
    consts
}

/// A constant for a port of kind `kind`, decoded from `value`.
fn held<T: Send + Sync + 'static>(value: &Lit, port: &str, kind: Kind<T>) -> Result<Held, String> {
    let decode = kind
        .decode
        .ok_or_else(|| format!("port `{port}` can't be set from a constant"))?;
    let decoded =
        decode(value).ok_or_else(|| format!("the constant for port `{port}` doesn't fit"))?;
    Ok(Held::new(|buf, slot| {
        ports::encode(decoded, kind, Keep::Fresh, &mut (), buf, slot);
    }))
}

/// Fixes input `at`'s constant, if the graph gave one, and registers how a later value for it
/// is decoded: the same way, so it is refused in the same words.
pub fn store_const<T: Send + Sync + 'static>(
    setup: &Setup<'_>,
    port: &str,
    at: usize,
    kind: Kind<T>,
    consts: &mut Consts,
) -> Result<(), String> {
    let Some(value) = setup.value(port) else {
        return Ok(());
    };
    consts.held[at] = Some(held(value, port, kind)?);
    let name = String::from(port);
    consts
        .live
        .read(setup.readers, setup.node, port, move |value| {
            held(value, &name, kind).map(|held| Arrived::Input(at, held))
        });
    Ok(())
}
