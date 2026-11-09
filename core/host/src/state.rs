//! Node state between ticks. The host owns it. A step's own state lives in the step's data, and
//! the runner never reads it. This store is read and written only between ticks, so no tick
//! path touches it.
//!
//! Each entry belongs to a node and an instance: two instances of one node id keep their state
//! apart. An entry is either plain state or a managed resource. A value is dropped once, when it
//! is replaced, removed, or the store is dropped.

mod resources;

use std::any::{Any, TypeId};

use daedalus_types::NodeId;

#[cfg(feature = "std")]
pub use crate::snapshot::{SnapshotError, restore_json, snapshot_json};

/// A state value that a snapshot can write and read back. `TAG` names the type in the snapshot.
pub trait StateCodec: Sized + Send + 'static {
    const TAG: &'static str;
    fn encode(&self) -> String;
    fn decode(text: &str) -> Option<Self>;
}

/// How one state type is written to and read from a snapshot.
#[derive(Clone, Copy)]
pub struct Codec {
    pub tag: &'static str,
    #[cfg_attr(not(feature = "std"), allow(dead_code))]
    pub(crate) encode: fn(&(dyn Any + Send)) -> Option<String>,
    #[cfg_attr(not(feature = "std"), allow(dead_code))]
    pub(crate) decode: fn(&str) -> Option<Box<dyn Any + Send>>,
}

impl Codec {
    pub fn of<T: StateCodec>() -> Codec {
        Codec {
            tag: T::TAG,
            encode: encode_as::<T>,
            decode: decode_as::<T>,
        }
    }
}

fn encode_as<T: StateCodec>(value: &(dyn Any + Send)) -> Option<String> {
    value.downcast_ref::<T>().map(StateCodec::encode)
}

fn decode_as<T: StateCodec>(text: &str) -> Option<Box<dyn Any + Send>> {
    T::decode(text).map(|value| Box::new(value) as Box<dyn Any + Send>)
}

struct Entry {
    node: NodeId,
    instance: u32,
    resource: bool,
    value: Box<dyn Any + Send>,
    codec: Option<Codec>,
}

/// Typed state for every node instance in one host.
#[derive(Default)]
pub struct StateStore {
    entries: Vec<Entry>,
}

impl StateStore {
    pub fn new() -> StateStore {
        StateStore::default()
    }

    /// Stores `value` as plain state of `node` instance `instance`. It replaces a value of the
    /// same type there. It is not written to snapshots.
    pub fn set<T: Send + 'static>(&mut self, node: NodeId, instance: u32, value: T) {
        self.put(node, instance, false, Box::new(value), None);
    }

    /// Stores snapshot-able state, replacing a value of the same type.
    pub fn set_encoded<T: StateCodec>(&mut self, node: NodeId, instance: u32, value: T) {
        self.put(
            node,
            instance,
            false,
            Box::new(value),
            Some(Codec::of::<T>()),
        );
    }

    /// The plain or encoded state of type `T` for this node instance.
    pub fn get<T: 'static>(&self, node: NodeId, instance: u32) -> Option<&T> {
        self.entries
            .iter()
            .filter(|entry| !entry.resource && entry.node == node && entry.instance == instance)
            .find_map(|entry| (*entry.value).downcast_ref::<T>())
    }

    /// As [`StateStore::get`], mutable.
    pub fn get_mut<T: 'static>(&mut self, node: NodeId, instance: u32) -> Option<&mut T> {
        self.entries
            .iter_mut()
            .filter(|entry| !entry.resource && entry.node == node && entry.instance == instance)
            .find_map(|entry| (*entry.value).downcast_mut::<T>())
    }

    /// Removes and returns the state of type `T` for this node instance.
    pub fn take<T: Send + 'static>(&mut self, node: NodeId, instance: u32) -> Option<T> {
        let at = self.position(node, instance, false, TypeId::of::<T>())?;
        let entry = self.entries.swap_remove(at);
        entry.value.downcast::<T>().ok().map(|boxed| *boxed)
    }

    /// Drops every entry of one node instance. It returns how many it dropped.
    pub fn remove_instance(&mut self, node: NodeId, instance: u32) -> usize {
        let before = self.entries.len();
        self.entries
            .retain(|entry| entry.node != node || entry.instance != instance);
        before - self.entries.len()
    }

    /// How many entries the store holds, state and resources both.
    /// Removes everything held for `node`, dropping each value once. Returns how many.
    pub fn remove_node(&mut self, node: NodeId) -> usize {
        let before = self.entries.len();
        self.entries.retain(|entry| entry.node != node);
        before - self.entries.len()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn put(
        &mut self,
        node: NodeId,
        instance: u32,
        resource: bool,
        value: Box<dyn Any + Send>,
        codec: Option<Codec>,
    ) {
        let type_id = (*value).type_id();
        match self.position(node, instance, resource, type_id) {
            Some(at) => {
                self.entries[at] = Entry {
                    node,
                    instance,
                    resource,
                    value,
                    codec,
                }
            }
            None => self.entries.push(Entry {
                node,
                instance,
                resource,
                value,
                codec,
            }),
        }
    }

    fn position(
        &self,
        node: NodeId,
        instance: u32,
        resource: bool,
        type_id: TypeId,
    ) -> Option<usize> {
        self.entries.iter().position(|entry| {
            entry.node == node
                && entry.instance == instance
                && entry.resource == resource
                && (*entry.value).type_id() == type_id
        })
    }

    /// Entries a snapshot writes: those with a codec, in store order.
    #[cfg_attr(not(feature = "std"), allow(dead_code))]
    pub(crate) fn encoded(&self) -> impl Iterator<Item = (NodeId, u32, Codec, String)> + '_ {
        self.entries.iter().filter_map(|entry| {
            let codec = entry.codec?;
            let text = (codec.encode)(&*entry.value)?;
            Some((entry.node, entry.instance, codec, text))
        })
    }

    #[cfg_attr(not(feature = "std"), allow(dead_code))]
    pub(crate) fn insert_decoded(
        &mut self,
        node: NodeId,
        instance: u32,
        codec: Codec,
        value: Box<dyn Any + Send>,
    ) {
        self.put(node, instance, false, value, Some(codec));
    }
}
