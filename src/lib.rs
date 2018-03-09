//! TODO document
#![warn(missing_docs)] // TODO turn this into deny

// TODO AGPL

extern crate futures_core;
extern crate indexmap;

#[cfg(test)]
extern crate atm_async_utils;

mod shared;
pub mod sync;
pub mod unsync;
