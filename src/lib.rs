//! This crate provides a wrapper around streams that allow multiple, independent
//! tasks to read from the same underlying stream. Each handle is assigned a key,
//! and each received item is given to a function to compute a key, determining
//! which handle receives the item. There is always default handle which receives
//! an item if the result of the key function does not match any other handle.
#![deny(missing_docs)]

extern crate futures;
extern crate ordermap;

#[cfg(test)]
extern crate quickcheck;
#[cfg(test)]
extern crate rand;
#[cfg(test)]
extern crate void;
#[cfg(test)]
extern crate atm_async_utils;

mod rc_mcs;
mod borrow_mcs;
mod shared;

pub use rc_mcs::*;
pub use borrow_mcs::*;
