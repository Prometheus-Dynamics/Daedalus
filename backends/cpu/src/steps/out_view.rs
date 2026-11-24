//! An output a node writes where its reader reads it: [`OutView`].
//!
//! A node takes `out: OutView<'_, T>` for an output that is a list of `T`. It says how much room it
//! needs ([`OutView::reserve`]), then writes its items: as numbers in place
//! ([`OutView::items_mut`]), or one record at a time through the items' wire form
//! ([`OutView::push`]). The output holds the list's wire form.
//!
//! When the planner placed the output in memory a worker lends (a space the worker's backend
//! offers), the room is a lease of that memory: the node's writes are the value the worker reads,
//! nothing is copied on the way, and nothing is allocated. Placed for a worker that lends none,
//! the node writes into memory the step keeps from tick to tick, and the list is handed on from
//! there as any wire-form output would be. Placed for CPU readers alone, the node writes into the
//! list itself, one the step keeps and refills once no reader holds it.
//!
//! Every write is checked: one past the room reserved, or a record whose write wrote another
//! length than its size pass said, is refused with words, and the step fails with them. A node
//! that needs more room after it started writing reserves again: the room grows in place when the
//! lent memory has it free after the room, and is refused otherwise.

use alloc::format;
use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::fmt;
use core::marker::PhantomData;

use daedalus_abi::{LeItem, Lease, Lend, Slot, StepCx};
use daedalus_runner::{INLINE_BYTES, UNSTAMPED, put_handed, put_inline, put_shared, put_typed};
use daedalus_types::WireForm;

use super::Keep;
use super::keep::{POOL_LIMIT, WireBuffers};

/// Why a write to an [`OutView`] was refused. The step fails with the same words.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refused(pub String);

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(feature = "std")]
impl std::error::Error for Refused {}

impl daedalus_abi::FixedError for Refused {
    fn code(&self) -> u32 {
        1
    }

    fn reason(&self) -> &str {
        &self.0
    }
}

impl From<Refused> for String {
    fn from(refused: Refused) -> String {
        refused.0
    }
}

/// Where a list's items start in memory the step keeps: a multiple of this from the allocation, so
/// they are aligned for any number.
const ITEM_ALIGN: usize = 8;

/// What an [`OutView`] output keeps between ticks: the lent room, if it has one, and memory to
/// write in otherwise.
#[derive(Default)]
pub struct OutSlot {
    /// Who lends room for this output, when it is placed in lent memory.
    room: Option<Arc<dyn Lend>>,
    lease: Option<Lease>,
    /// The output is written as its wire form; otherwise as the list itself.
    wire: bool,
    /// Only nodes of the same plugin read the output, as output `at` of the step: the list goes
    /// to them in a box of the plugin's own.
    carried: Option<usize>,
    scratch: Vec<u8>,
    /// Where the value starts in `scratch`.
    start: usize,
    /// Wire buffers handed on before, reused once no one reads them.
    pool: WireBuffers,
    /// The bytes before the items: a list's `u32` count, or none for bytes.
    header: usize,
    /// The bytes reserved, the header's with them; 0 before the node reserves.
    reserved: usize,
    /// The bytes written, the header's with them.
    written: usize,
    items: usize,
    refused: Option<String>,
    /// `output {port} of {node}`, for the words of a refusal.
    named: String,
    /// For an output CPU readers alone read: the lists handed on before, as
    /// `Vec<Arc<Vec<T>>>`, the one this tick writes first.
    lists: Option<Box<dyn core::any::Any + Send + Sync>>,
    /// This tick's list is picked.
    picked: bool,
    /// The picked list still holds an earlier tick's items.
    stale: bool,
}

impl OutSlot {
    /// For output `at` of the node `setup` builds: in the room lent for it, if any.
    pub fn lend(&mut self, setup: &super::Setup<'_>, at: usize, port: &str) {
        let port_ref = daedalus_backend::PortRef {
            node: setup.node,
            port: daedalus_types::PortIndex(at as u16),
        };
        self.room = setup
            .lent
            .iter()
            .find(|room| room.port == port_ref)
            .map(|room| room.room.clone());
        self.wire = setup.keep.get(at) == Some(&Keep::Wire);
        self.carried = (setup.keep.get(at) == Some(&Keep::Carried)).then_some(at);
        self.named = format!("output {port} of {}", setup.id);
    }

    /// Starts a tick's value. A lease left from a tick that failed is let go.
    /// Releases what the output's cell holds from the tick before, so the memory it was written
    /// in is free to write again: the list no reader holds, or the lent room.
    pub fn ready(&mut self, cell: &mut Slot) {
        daedalus_runner::put(cell, Slot::EMPTY);
    }

    fn begin(&mut self, byte: bool) {
        self.lease = None;
        self.picked = false;
        self.refused = None;
        self.header = if byte { 0 } else { 4 };
        self.reserved = 0;
        self.written = 0;
        self.items = 0;
    }

    /// Lets go of what a failed tick wrote: its lease goes back to the lent memory.
    pub fn abandon(&mut self) {
        self.lease = None;
    }

    fn refuse(&mut self, why: String) -> Refused {
        let why = format!("{} {why}", self.named);
        self.refused = Some(why.clone());
        Refused(why)
    }

    /// Whether the room is lent memory.
    fn lent(&self) -> bool {
        self.lease.is_some()
    }

    /// This tick's list, for an output CPU readers alone read: a list no reader holds any more,
    /// emptied, or a new one.
    fn list<T: Send + Sync + 'static>(&mut self) -> &mut Vec<T> {
        let known = self
            .lists
            .as_ref()
            .is_some_and(|lists| lists.is::<Vec<Arc<Vec<T>>>>());
        if !known {
            self.lists = Some(alloc::boxed::Box::new(Vec::<Arc<Vec<T>>>::new()));
            self.picked = false;
        }
        let lists = self
            .lists
            .as_mut()
            .and_then(|lists| lists.downcast_mut::<Vec<Arc<Vec<T>>>>());
        let Some(lists) = lists else {
            unreachable!("the lists were just made of this type")
        };
        if !self.picked || lists.is_empty() {
            match lists.iter().position(|list| Arc::strong_count(list) == 1) {
                Some(free) => lists.swap(0, free),
                None => {
                    lists.insert(0, Arc::new(Vec::new()));
                    lists.truncate(POOL_LIMIT);
                }
            }
            self.picked = true;
            self.stale = true;
        }
        if Arc::get_mut(&mut lists[0]).is_none() {
            lists[0] = Arc::new(Vec::new());
        }
        match Arc::get_mut(&mut lists[0]) {
            Some(list) => list,
            None => unreachable!("a list just made has one holder"),
        }
    }

    /// Room for `total` bytes, the header's with them.
    fn reserve(&mut self, total: usize) -> Result<(), Refused> {
        if let Some(why) = &self.refused {
            return Err(Refused(why.clone()));
        }
        if total <= self.reserved {
            return Ok(());
        }
        if !self.wire {
            // The list itself is written: the bytes are counted, to refuse a write past them.
            self.reserved = total;
            return Ok(());
        }
        if let Some(lease) = self.lease.as_mut() {
            if lease.grow(total) {
                self.reserved = total;
                return Ok(());
            }
            let cap = lease.cap();
            return Err(self.refuse(format!(
                "grew to {total} bytes, past the {cap} its lent room holds: reserve it all before writing"
            )));
        }
        if self.reserved == 0
            && let Some(room) = &self.room
            && total >= room.threshold()
            && let Some(lease) = room.lease(total, self.header)
        {
            self.lease = Some(lease);
            self.reserved = total;
            return Ok(());
        }
        // Memory the step keeps: grown when needed, and reused, so a steady size allocates once.
        let written = self.start..self.start + self.written;
        self.scratch.resize(total + ITEM_ALIGN, 0);
        let address = self.scratch.as_ptr() as usize + self.header;
        let start = (ITEM_ALIGN - address % ITEM_ALIGN) % ITEM_ALIGN;
        if start != self.start {
            self.scratch.copy_within(written, start);
            self.start = start;
        }
        self.reserved = total;
        Ok(())
    }

    /// The room, `reserved` bytes long.
    fn room(&mut self) -> &mut [u8] {
        match self.lease.as_mut() {
            Some(lease) => &mut lease.bytes_mut()[..self.reserved],
            None => &mut self.scratch[self.start..self.start + self.reserved],
        }
    }

    /// Writes the tick's value, a list of `T`, into `cell`: the lent room sealed into a
    /// hand-over, what the step's memory holds as wire form, or the list itself. False, with the
    /// fault recorded, when a write was refused.
    pub fn emit<T: WireForm + Clone + Send + Sync + 'static>(
        &mut self,
        cell: &mut Slot,
        cx: &mut StepCx,
        node: u32,
    ) -> bool {
        if let Some(why) = self.refused.take() {
            self.lease = None;
            cx.fault(Some(node), &why);
            return false;
        }
        let (ty, space) = UNSTAMPED;
        if !self.wire {
            self.list::<T>();
            let handed = self
                .lists
                .as_ref()
                .and_then(|lists| lists.downcast_ref::<Vec<Arc<Vec<T>>>>())
                .and_then(|lists| lists.first().cloned());
            let list = match (handed, self.carried) {
                (Some(list), Some(at)) => {
                    daedalus_runner::hand_inside(list, None, cx, at, cell).err()
                }
                (handed, _) => handed,
            };
            if let Some(list) = list {
                put_typed(cell, ty, space, list);
            }
            return true;
        }
        if self.reserved == 0 && self.reserve(self.header).is_err() {
            return false;
        }
        if self.header == 4 {
            let count = u32::try_from(self.items).unwrap_or(u32::MAX).to_le_bytes();
            self.room()[..4].copy_from_slice(&count);
            self.written = self.written.max(4);
        }
        if let Some(lease) = self.lease.take() {
            if let Some(bytes) = lease.seal(self.written) {
                put_handed(cell, ty, space, bytes);
                return true;
            }
            cx.fault(Some(node), "an output view wrote more than its room");
            return false;
        }
        let bytes = &self.scratch[self.start..self.start + self.written];
        if bytes.len() <= INLINE_BYTES {
            let _ = put_inline(cell, ty, space, bytes);
            return true;
        }
        let free = self
            .pool
            .iter()
            .position(|held| held.len() == bytes.len() && Arc::strong_count(held) == 1)
            .map(|at| self.pool.swap_remove(at));
        let shared = match free {
            Some(mut held) => match Arc::get_mut(&mut held) {
                Some(room) => {
                    room.copy_from_slice(bytes);
                    held
                }
                None => Arc::from(bytes),
            },
            None => Arc::from(bytes),
        };
        self.pool.push(shared.clone());
        if self.pool.len() > POOL_LIMIT {
            self.pool.swap_remove(0);
        }
        put_shared(cell, ty, space, shared);
        true
    }
}

/// An output that is a list of `T`, written in place: see the module.
pub struct OutView<'a, T: Send + Sync + 'static> {
    slot: &'a mut OutSlot,
    /// For an output CPU readers alone read: the list being written, out of the slot until the
    /// view is dropped.
    list: Option<Vec<T>>,
    _of: PhantomData<fn(T)>,
}

impl<T: Send + Sync + 'static> OutView<'_, T> {
    /// The list being written, for an output CPU readers alone read, emptied of an earlier
    /// tick's items.
    fn list(&mut self) -> &mut Vec<T> {
        let stale = core::mem::take(&mut self.slot.stale);
        let list = self
            .list
            .get_or_insert_with(|| core::mem::take(self.slot.list::<T>()));
        if stale || core::mem::take(&mut self.slot.stale) {
            list.clear();
        }
        list
    }

    /// The list being written, as it is: an earlier tick's items may still be in it.
    fn kept(&mut self) -> &mut Vec<T> {
        let list = self
            .list
            .get_or_insert_with(|| core::mem::take(self.slot.list::<T>()));
        self.slot.stale = false;
        list
    }
}

impl<T: Send + Sync + 'static> Drop for OutView<'_, T> {
    fn drop(&mut self) {
        if let Some(list) = self.list.take() {
            *self.slot.list::<T>() = list;
        }
    }
}

impl<'a, T: WireForm + Clone + Send + Sync + 'static> OutView<'a, T> {
    #[doc(hidden)]
    pub fn new(slot: &'a mut OutSlot) -> OutView<'a, T> {
        slot.begin(T::BYTE);
        OutView {
            slot,
            list: None,
            _of: PhantomData,
        }
    }

    /// Room for `n` items, for items whose wire form is their memory (`WireForm::FLAT`), so each
    /// is as long as `T`. Reserving again for more grows the room.
    pub fn reserve(&mut self, n: usize) -> Result<(), Refused> {
        if !T::FLAT {
            return Err(self.slot.refuse(format!(
                "reserves {n} items, but {}'s wire form has no fixed size: reserve_bytes with the items' wire length",
                core::any::type_name::<T>()
            )));
        }
        self.reserve_bytes(n.saturating_mul(core::mem::size_of::<T>()))?;
        if !self.slot.wire {
            let more = n.saturating_sub(self.slot.items);
            self.list().reserve(more);
        }
        Ok(())
    }

    /// Room for items whose wire forms take `bytes` bytes in all.
    pub fn reserve_bytes(&mut self, bytes: usize) -> Result<(), Refused> {
        let total = self.slot.header.saturating_add(bytes);
        self.slot.reserve(total)
    }

    /// Writes `item` after the items written so far, through its wire form: its size pass first,
    /// then its write, which must write what the size pass said.
    pub fn push(&mut self, item: &T) -> Result<(), Refused> {
        if let Some(why) = &self.slot.refused {
            return Err(Refused(why.clone()));
        }
        let need = item.wire_len();
        let (len, reserved) = (self.slot.written.max(self.slot.header), self.slot.reserved);
        if len.saturating_add(need) > reserved {
            let past = len.saturating_add(need) - reserved;
            let room = reserved.saturating_sub(self.slot.header);
            return Err(self.slot.refuse(format!(
                "wrote {past} bytes past the {room} it reserved: reserve more before writing"
            )));
        }
        if !self.slot.wire {
            self.list().push(item.clone());
            self.slot.written = len + need;
            self.slot.items += 1;
            return Ok(());
        }
        let wrote = item.write_wire(&mut self.slot.room()[len..reserved]);
        if wrote != need {
            return Err(self.slot.refuse(format!(
                "wrote an item of {wrote} bytes where {}'s size pass said {need}",
                core::any::type_name::<T>()
            )));
        }
        self.slot.written = len + wrote;
        self.slot.items += 1;
        Ok(())
    }

    /// Writes each of `items` after the items written so far, as [`push`](Self::push) does one.
    pub fn extend(&mut self, items: impl IntoIterator<Item = T>) -> Result<(), Refused> {
        if let Some(why) = &self.slot.refused {
            return Err(Refused(why.clone()));
        }
        let (header, reserved) = (self.slot.header, self.slot.reserved);
        let (mut len, mut count) = (self.slot.written.max(header), self.slot.items);
        let mut past = 0;
        let mut wrong = None;
        if self.slot.wire {
            // The room is found once, and each item written straight into it.
            let room = self.slot.room();
            for item in items {
                let need = item.wire_len();
                if len.saturating_add(need) > reserved {
                    past = len.saturating_add(need) - reserved;
                    break;
                }
                let wrote = item.write_wire(&mut room[len..len + need]);
                if wrote != need {
                    wrong = Some((wrote, need));
                    break;
                }
                len += need;
                count += 1;
            }
        } else if T::FLAT {
            // Every item takes `T`'s size, so how many fit is known before writing any.
            let size = core::mem::size_of::<T>().max(1);
            let fit = reserved.saturating_sub(len) / size;
            let mut items = items.into_iter();
            let list = self.list();
            let before = list.len();
            list.extend(items.by_ref().take(fit));
            let wrote = list.len() - before;
            len += wrote * size;
            count += wrote;
            if items.next().is_some() {
                past = (len + size).saturating_sub(reserved);
            }
        } else {
            let list = self.list();
            for item in items {
                let need = item.wire_len();
                if len.saturating_add(need) > reserved {
                    past = len.saturating_add(need) - reserved;
                    break;
                }
                list.push(item);
                len += need;
                count += 1;
            }
        }
        self.slot.written = len;
        self.slot.items = count;
        if let Some((wrote, need)) = wrong {
            return Err(self.slot.refuse(format!(
                "wrote an item of {wrote} bytes where {}'s size pass said {need}",
                core::any::type_name::<T>()
            )));
        }
        if past > 0 {
            let room = reserved.saturating_sub(header);
            return Err(self.slot.refuse(format!(
                "wrote {past} bytes past the {room} it reserved: reserve more before writing"
            )));
        }
        Ok(())
    }

    /// How many items are written.
    pub fn len(&self) -> usize {
        self.slot.items
    }

    pub fn is_empty(&self) -> bool {
        self.slot.items == 0
    }

    /// Whether the room is memory a reader lends, so the items are written where it reads them.
    pub fn in_place(&self) -> bool {
        self.slot.lent()
    }
}

impl<T: WireForm + LeItem + Send + Sync> OutView<'_, T> {
    /// The list as `n` numbers to write in place, reserving room for them first. The output is
    /// these `n` numbers, whatever was written before.
    pub fn items_mut(&mut self, n: usize) -> Result<&mut [T], Refused> {
        let header = self.slot.header;
        let end = header + n * core::mem::size_of::<T>();
        if !self.slot.wire {
            self.reserve_bytes(n.saturating_mul(core::mem::size_of::<T>()))?;
            let Some((zero, _)) = T::read_wire(&[0; 16]) else {
                return Err(self
                    .slot
                    .refuse(String::from("has no zero to start its numbers from")));
            };
            self.slot.written = end;
            self.slot.items = n;
            // The numbers are the node's to write, as in lent memory: an earlier tick's are left
            // where the list is as long, and only the rest are started at zero.
            let list = self.kept();
            list.truncate(n);
            list.resize(n, zero);
            return Ok(&mut list[..]);
        }
        self.reserve(n)?;
        if daedalus_abi::le_items_mut::<T>(&mut self.slot.room()[header..end]).is_none() {
            let why = format!(
                "{} can't write numbers in place on this machine",
                self.slot.named
            );
            self.slot.refused = Some(why.clone());
            return Err(Refused(why));
        }
        self.slot.written = end;
        self.slot.items = n;
        Ok(daedalus_abi::le_items_mut::<T>(&mut self.slot.room()[header..end]).unwrap_or_default())
    }
}
