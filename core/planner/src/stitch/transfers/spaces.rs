//! The memory spaces of each backend, by index, and where each backend's outputs can be written.

use daedalus_backend::{Backend, MemorySpace, SpaceKind};

use super::Routing;

/// One backend's memory, by space index.
pub(super) struct BackendSpaces {
    pub(super) list: Vec<(MemorySpace, usize)>,
    /// The other spaces an output can be written in, by index, in the backend's order.
    pub(super) placements: Vec<usize>,
}

impl BackendSpaces {
    pub(super) fn home(&self) -> Option<usize> {
        self.list.first().map(|(_, at)| *at)
    }

    /// Every space an output of this backend can be written in: its first, then the others.
    pub(super) fn writable(&self) -> Vec<usize> {
        let mut spaces: Vec<usize> = self.home().into_iter().collect();
        for &at in &self.placements {
            if !spaces.contains(&at) {
                spaces.push(at);
            }
        }
        spaces
    }

    pub(super) fn readable(&self) -> Vec<usize> {
        self.list
            .iter()
            .filter(|(s, _)| s.readable)
            .map(|(_, at)| *at)
            .collect()
    }

    pub(super) fn host_kind(&self) -> Option<usize> {
        self.list
            .iter()
            .find(|(s, _)| s.kind == SpaceKind::Host)
            .map(|(_, at)| *at)
    }
}

impl BackendSpaces {
    /// Each backend's spaces, by index in `routing`. A placement is one of a backend's own spaces,
    /// or one another backend offers when it takes offers.
    pub(super) fn all(routing: &mut Routing, backends: &[&dyn Backend]) -> Vec<BackendSpaces> {
        let lists: Vec<Vec<(MemorySpace, usize)>> = backends
            .iter()
            .map(|backend| {
                backend
                    .memory()
                    .into_iter()
                    .map(|space| {
                        let at = routing.space(&space.id);
                        (space, at)
                    })
                    .collect()
            })
            .collect();
        // The spaces each backend offers other backends' nodes to write into: its own, readable ones.
        let offered: Vec<Vec<usize>> = backends
            .iter()
            .zip(&lists)
            .map(|(backend, list)| {
                backend
                    .offers()
                    .iter()
                    .filter_map(|id| {
                        list.iter()
                            .find(|(space, _)| space.id == *id && space.readable)
                    })
                    .map(|(_, at)| *at)
                    .collect()
            })
            .collect();
        backends
            .iter()
            .zip(lists)
            .enumerate()
            .map(|(rank, (backend, list))| {
                // A placement is one of the backend's own spaces, or one another backend offers.
                let mut placements: Vec<usize> = backend
                    .placements()
                    .iter()
                    .filter_map(|id| list.iter().find(|(space, _)| space.id == *id))
                    .map(|(_, at)| *at)
                    .collect();
                if backend.takes_offers() {
                    let others = offered
                        .iter()
                        .enumerate()
                        .filter(|(other, _)| *other != rank)
                        .flat_map(|(_, spaces)| spaces.iter().copied());
                    placements.extend(others);
                }
                BackendSpaces { list, placements }
            })
            .collect()
    }
}
