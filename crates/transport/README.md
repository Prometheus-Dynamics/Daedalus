# daedalus-transport

Generic payload transport primitives for Daedalus.

This crate owns the low-level vocabulary for the new transport model: stable type keys, access modes, residency/layout metadata, adapter costs, and type-erased payload storage. It intentionally does not depend on runtime, registry, GPU, FFI, or data crates.
