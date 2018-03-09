//! A single-threaded multi-consumer-stream.

use std::cell::RefCell;
use std::hash::Hash;
use std::rc::Rc;

use futures_core::{Poll, Stream};
use futures_core::task::Context;

use shared::Shared;

/// Wraps a stream and provides a multi-consumer-stream.
///
/// This structs allows to create new `MCSHandle`, which receive all the items/errors for which
/// the `ItemFn`/`ErrFn` compute a certain key. All items/errors for which no corresponding
/// `MCSHandle` exist are routed to the primary `MCS`.
pub struct MCS<S: Stream, Key: Eq + Hash + Copy, ItemFn, ErrFn>(Rc<RefCell<Shared<S,
                                                                                   Key,
                                                                                   ItemFn,
                                                                                   ErrFn>>>);

impl<S, Key, ItemFn, ErrFn> MCS<S, Key, ItemFn, ErrFn>
    where S: Stream,
          Key: Eq + Hash + Copy
{
    /// Create a new `MCS` with the given functions to compute keys.
    pub fn new(stream: S, item_fn: ItemFn, err_fn: ErrFn) -> MCS<S, Key, ItemFn, ErrFn> {
        MCS(Rc::new(RefCell::new(Shared::new(stream, item_fn, err_fn))))
    }
}

impl<S, Key, ItemFn, ErrFn> MCS<S, Key, ItemFn, ErrFn>
    where S: Stream,
          Key: Eq + Hash + Copy,
          ItemFn: Fn(&S::Item) -> Key,
          ErrFn: Fn(&S::Error) -> Key
{
    /// Consume the `MCS` and retrieve ownership of the wrapped stream.
    ///
    /// Polling an `MCSHandle` after consuming its `MCS` panics.
    pub fn into_inner(self) -> S {
        self.0.borrow_mut().inner.take().unwrap()
    }

    /// Create a `MCSHandle` to the underlying stream. The handle receives all items for which the
    /// `MCS`'s `item_fn` returns `key`, and all errors for which the `MCS`'s `error_fn` returns
    /// `key`.
    ///
    /// Panics if there is already a handle for that key.
    pub fn mcs_handle(&self, key: Key) -> MCSHandle<S, Key, ItemFn, ErrFn> {
        MCSHandle::new(self.0.clone(), key)
    }

    /// Create a `MCSHandle` to the underlying stream. The handle receives all items for which the
    /// `MCS`'s `item_fn` returns `key`, and all errors for which the `MCS`'s `error_fn` returns
    /// `key`.
    ///
    /// This returns `None` if there is already a handle for that key.
    pub fn try_mcs_handle(&self, key: Key) -> Option<MCSHandle<S, Key, ItemFn, ErrFn>> {
        MCSHandle::try_new(self.0.clone(), key)
    }
}

impl<S, Key, ItemFn, ErrFn> Stream for MCS<S, Key, ItemFn, ErrFn>
    where S: Stream,
          Key: Copy + Eq + Hash,
          ItemFn: Fn(&S::Item) -> Key,
          ErrFn: Fn(&S::Error) -> Key
{
    type Item = S::Item;
    type Error = S::Error;

    fn poll_next(&mut self, cx: &mut Context) -> Poll<Option<Self::Item>, Self::Error> {
        self.0.borrow_mut().poll_default(cx)
    }
}

/// A stream that receives all items/errors for which the corresponding `MCS`'s `item_fn`/`err_fn`
/// return the key from which the `MCSHandle` was constructed.
pub struct MCSHandle<S: Stream, Key: Eq + Hash + Copy, ItemFn, ErrFn> {
    shared: Rc<RefCell<Shared<S, Key, ItemFn, ErrFn>>>,
    key: Key,
}

impl<S, Key, ItemFn, ErrFn> MCSHandle<S, Key, ItemFn, ErrFn>
    where S: Stream,
          Key: Eq + Hash + Copy
{
    fn new(shared: Rc<RefCell<Shared<S, Key, ItemFn, ErrFn>>>,
           key: Key)
           -> MCSHandle<S, Key, ItemFn, ErrFn> {
        assert!(shared.borrow_mut().register_key(key.clone()),
                "Tried to register duplicate handles");
        MCSHandle { shared, key }
    }

    fn try_new(shared: Rc<RefCell<Shared<S, Key, ItemFn, ErrFn>>>,
               key: Key)
               -> Option<MCSHandle<S, Key, ItemFn, ErrFn>> {
        if shared.borrow_mut().register_key(key.clone()) {
            Some(MCSHandle { shared, key })
        } else {
            None
        }
    }

    /// Create a `MCSHandle` to the underlying stream. The handle receives all items for which the
    /// corresponding `MCS`'s `item_fn` returns `key`, and all errors for which the `MCS`'s
    /// `error_fn` returns `key`.
    ///
    /// Panics if there is already a handle for that key.
    pub fn mcs_handle(&self, key: Key) -> MCSHandle<S, Key, ItemFn, ErrFn> {
        MCSHandle::new(self.shared.clone(), key)
    }

    /// Create a `MCSHandle` to the underlying stream. The handle receives all items for which the
    /// corresponding `MCS`'s `item_fn` returns `key`, and all errors for which the `MCS`'s
    /// `error_fn` returns `key`.
    ///
    /// This returns `None` if there is already a handle for that key.
    pub fn try_mcs_handle(&self, key: Key) -> Option<MCSHandle<S, Key, ItemFn, ErrFn>> {
        MCSHandle::try_new(self.shared.clone(), key)
    }
}

impl<S, Key, ItemFn, ErrFn> Drop for MCSHandle<S, Key, ItemFn, ErrFn>
    where S: Stream,
          Key: Eq + Hash + Copy
{
    /// Deregisters the key of this `KeyMCS`.
    fn drop(&mut self) {
        self.shared.borrow_mut().deregister_key(&self.key);
    }
}

impl<S, Key, ItemFn, ErrFn> Stream for MCSHandle<S, Key, ItemFn, ErrFn>
    where S: Stream,
          Key: Eq + Hash + Copy,
          ItemFn: Fn(&S::Item) -> Key,
          ErrFn: Fn(&S::Error) -> Key
{
    type Item = S::Item;
    type Error = S::Error;

    fn poll_next(&mut self, cx: &mut Context) -> Poll<Option<Self::Item>, Self::Error> {
        self.shared.borrow_mut().poll_handle(self.key, cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
