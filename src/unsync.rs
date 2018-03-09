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

    use atm_async_utils::test_channel::*;
    use futures::{SinkExt, StreamExt, FutureExt, Never};
    use futures::sink::close;
    use futures::stream::iter_ok;
    use futures::executor::block_on;

    #[test]
    fn it_works() {
        let (sender, receiver) = test_channel(2);

        let default: MCS<TestReceiver<u32, u32>, u8, fn(&u32) -> u8, fn(&u32) -> u8> =
            MCS::new(receiver,
                     |x| match x {
                         y if y % 3 == 0 => 1,
                         y if y % 5 == 0 => 2,
                         _ => 0,
                     },
                     |x| match x {
                         y if y % 3 == 0 => 1,
                         y if y % 5 == 0 => 2,
                         _ => 0,
                     });
        let s1 = default.mcs_handle(1);
        let s2 = default.mcs_handle(2);

        let sending = sender
            .send_all(iter_ok::<_, Never>(0..40).map(|i| if i % 2 == 0 {
                                                         Ok(i / 2)
                                                     } else {
                                                         Err(i / 2)
                                                     }))
            .and_then(|(sender, _)| close(sender));

        let (_, threes, fives, defaults) =
            block_on(sending.join4(s1.then(|result| match result {
                                               Ok(foo) => Ok(Ok(foo)),
                                               Err(err) => Ok(Err(err)),
                                           })
                                       .collect(),
                                   s2.then(|result| match result {
                                               Ok(foo) => Ok(Ok(foo)),
                                               Err(err) => Ok(Err(err)),
                                           })
                                       .collect(),
                                   default
                                       .then(|result| match result {
                                                 Ok(foo) => Ok(Ok(foo)),
                                                 Err(err) => Ok(Err(err)),
                                             })
                                       .collect()))
                    .unwrap();

        assert_eq!(threes,
                   vec![Ok(0), Err(0), Ok(3), Err(3), Ok(6), Err(6), Ok(9), Err(9), Ok(12),
                        Err(12), Ok(15), Err(15), Ok(18), Err(18)]);
        assert_eq!(fives, vec![Ok(5), Err(5), Ok(10), Err(10)]);
        assert_eq!(defaults,
                   vec![Ok(1), Err(1), Ok(2), Err(2), Ok(4), Err(4), Ok(7), Err(7), Ok(8),
                        Err(8), Ok(11), Err(11), Ok(13), Err(13), Ok(14), Err(14), Ok(16),
                        Err(16), Ok(17), Err(17), Ok(19), Err(19)]);
    }
}
