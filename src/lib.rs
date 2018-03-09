//! Provides a multi-consumer-stream, that allows to route certain items of a wrapped stream to independent handles.
#![deny(missing_docs)]

extern crate futures_core;
extern crate indexmap;

#[cfg(test)]
extern crate atm_async_utils;
#[cfg(test)]
extern crate futures;

mod shared;
// pub mod sync;
mod unsync;

pub use unsync::*;
