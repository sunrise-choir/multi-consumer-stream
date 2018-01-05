use std::cell::RefCell;
use std::rc::Rc;

use futures::{Stream, Poll};

use shared::*;

/// A wrapper for a stream, that allows to create handles (of type `MCS`) which
/// receive certain items of the stream, based on a function applied to each item.
pub struct DefaultMCS<S, K, F> {}

impl<S, K, F> DefaultMCS<S, K, F>
    where S: Stream,
          F: Fn(S::Item) -> K
{
    /// Create a new `DefaultMCS`. The `fun` is applied to each item in the stream.
    /// If the return value matches the key of another handle, the item is given
    /// to that handle rather than being emitted on the `DefaultMCS`.
    pub fn new(stream: S, key_fun: F) -> DefaultMCS<S, K, F> {
        unimplemented!()
    }

    /// Create a new handle to the same underlying sink, which receives all items
    /// for which the key_fun of the `DefaultMCS` returns `key`.
    pub fn handle(&self, key: K) -> MCS<S, K, F> {
        unimplemented!()
    }
}

impl<S, K, F> Stream for DefaultMCS<S, K, F>
    where S: Stream,
          F: Fn(S::Item) -> K
{
    type Item = S::Item;
    type Error = S::Error;

    /// Receives each item of the wrapped stream for which the result of the
    /// key_fun does not match the key of a handle.
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        unimplemented!()
    }
}

/// A handle to a `DefaultMCS`, which receives only the items of the underlying
/// stream for which the key_fn returns the correct key.
///
/// This can also be used to obtain further handles with different keys.
pub struct MCS<S, K, F> {}

impl<S, K, F> MCS<S, K, F>
    where S: Stream,
          F: Fn(S::Item) -> K
{
    /// Create a new handle to the same underlying sink, which receives all items
    /// for which the key_fun of the `DefaultMCS` returns `key`.
    pub fn handle(&self, key: K) -> MCS<S, K, F> {
        unimplemented!()
    }
}

impl<S, K, F> Stream for MCS<S, K, F>
    where S: Stream,
          F: Fn(S::Item) -> K
{
    type Item = S::Item;
    type Error = S::Error;

    /// Receives each item of the wrapped stream for which the result of the
    /// key_fun matches the key of this handle.
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        unimplemented!()
    }
}
