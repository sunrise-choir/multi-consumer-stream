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

use std::cell::RefCell;
use std::collections::HashSet;
use std::hash::Hash;

use futures::{Stream, Poll, Async};
use futures::task::{Task, current};
use ordermap::OrderMap;

/// A wrapper around a stream, that allows obtaining multiple handles for
/// reading from that stream.
///
/// A `KeyHandle` only receives items for which the `key_fn` returns a
/// certain key. All other items are emitted on a `DefaultHandle`.
pub struct OwnerMCS<S: Stream, K, F>(RefCell<Shared<S, K, F>>);

impl<S, K, F> OwnerMCS<S, K, F>
    where S: Stream,
          K: Copy + Eq + Hash,
          F: Fn(S::Item) -> (S::Item, K)
{
    /// Create a new `OwnerMCS`, wrapping `stream`.
    ///
    /// The `key_fn` is invoked for each item emitted by the stream. Its return
    /// value is used to give the item to the correct handle. If the return
    /// value does not match the key of any handle, the item is emitted on a
    /// `DefaultHandle`.
    pub fn new(stream: S, key_fn: F) -> OwnerMCS<S, K, F> {
        OwnerMCS(RefCell::new(Shared::new(stream, key_fn)))
    }

    /// Create a `KeyHandle` to the underlying stream. The handle receives all
    /// items for which the `key_fn` returns `key`.
    ///
    /// Panics if there is already a handle for that key.
    pub fn key_handle(&self, key: K) -> KeyHandle<S, K, F> {
        KeyHandle::new(self, key)
    }

    /// Create a `KeyHandle` to the underlying stream. The handle receives all
    /// items for which the `key_fn` returns `key`. This returns `None` if there
    /// is already a handle for the given key.
    pub fn try_key_handle(&self, key: K) -> Option<KeyHandle<S, K, F>> {
        KeyHandle::try_new(self, key)
    }

    /// Create a `DefaultHandle` to the underlying stream. The handle receives
    /// all items for which there is no `KeyHandle`.
    ///
    /// The behaviour of multiple `DefaultHandles` polling at the same time is
    /// safe, but unspecified. Just don't do that.
    pub fn default_handle(&self) -> DefaultHandle<S, K, F> {
        DefaultHandle::new(self)
    }
}

/// A `DefaultHandle` to an `OwnerMCS`, receiving all items for which the owner's
/// `key_fn` return a key not associated with the any `KeyHandle`.
///
/// Not consuming a `DefaultHandle` as a stream will block all consumers of the stream
/// once an item for which there is no `KeyHandle` comes up.
///
/// The behaviour of multiple `DefaultHandles` polling at the same time is
/// safe, but unspecified. Just don't do that.
pub struct DefaultHandle<'owner, S: 'owner, K: 'owner, F: 'owner>(&'owner OwnerMCS<S, K, F>)
    where S: Stream;

impl<'owner, S, K, F> DefaultHandle<'owner, S, K, F>
    where S: Stream,
          K: Copy + Eq + Hash,
          F: Fn(S::Item) -> (S::Item, K)
{
    fn new(owner: &'owner OwnerMCS<S, K, F>) -> DefaultHandle<'owner, S, K, F> {
        DefaultHandle(owner)
    }

    /// Create a `KeyHandle` to the underlying stream. The handle receives all
    /// items for which the `key_fn` returns `key`.
    ///
    /// Panics if there is already a handle for that key.
    pub fn key_handle(&self, key: K) -> KeyHandle<S, K, F> {
        KeyHandle::new(self.0, key)
    }

    /// Create a `KeyHandle` to the underlying stream. The handle receives all
    /// items for which the `key_fn` returns `key`. This returns `None` if there
    /// is already a handle for the given key.
    pub fn try_key_handle(&self, key: K) -> Option<KeyHandle<S, K, F>> {
        KeyHandle::try_new(self.0, key)
    }

    /// Create a `DefaultHandle` to the underlying stream. The handle receives
    /// all items for which there is no `KeyHandle`.
    ///
    /// The behaviour of multiple `DefaultHandles` polling at the same time is
    /// safe, but unspecified. Just don't do that.
    pub fn default_handle(&self) -> DefaultHandle<S, K, F> {
        DefaultHandle::new(self.0)
    }
}

impl<'owner, S, K, F> Stream for DefaultHandle<'owner, S, K, F>
    where S: Stream,
          K: Copy + Eq + Hash,
          F: Fn(S::Item) -> (S::Item, K)
{
    type Item = S::Item;
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        (self.0).0.borrow_mut().poll_default()
    }
}

/// A `KeyHandle` to an `OwnerMCS`, receiving only the items for which the owner's
/// `key_fn` return the key associated with the `KeyHandle`.
///
/// Not consuming a `KeyHandle` as a stream will block all consumers of the stream
/// once a matching item comes up.
pub struct KeyHandle<'owner, S: 'owner, K: 'owner, F: 'owner>
    where S: Stream,
          K: Eq + Hash
{
    owner: &'owner OwnerMCS<S, K, F>,
    key: K,
}

impl<'owner, S, K, F> KeyHandle<'owner, S, K, F>
    where S: Stream,
          K: Copy + Eq + Hash,
          F: Fn(S::Item) -> (S::Item, K)
{
    fn new(owner: &'owner OwnerMCS<S, K, F>, key: K) -> KeyHandle<'owner, S, K, F> {
        assert!(owner.0.borrow_mut().register_key(key),
                "Tried to register duplicate handles");
        KeyHandle { owner, key }
    }

    fn try_new(owner: &'owner OwnerMCS<S, K, F>, key: K) -> Option<KeyHandle<'owner, S, K, F>> {
        if owner.0.borrow_mut().register_key(key) {
            Some(KeyHandle { owner, key })
        } else {
            None
        }
    }

    /// Create a `KeyHandle` to the underlying stream. The handle receives all
    /// items for which the `key_fn` returns `key`.
    ///
    /// Panics if there is already a handle for that key.
    pub fn handle(&self, key: K) -> KeyHandle<S, K, F> {
        KeyHandle::new(self.owner, key)
    }

    /// Create a `KeyHandle` to the underlying stream. The handle receives all
    /// items for which the `key_fn` returns `key`. This returns `None` if there
    /// is already a handle for the given key.
    pub fn try_handle(&self, key: K) -> Option<KeyHandle<S, K, F>> {
        KeyHandle::try_new(self.owner, key)
    }
}

impl<'owner, S, K, F> Drop for KeyHandle<'owner, S, K, F>
    where S: Stream,
          K: Eq + Hash
{
    /// Deregisters the key of this `KeyHandle`.
    fn drop(&mut self) {
        self.owner.0.borrow_mut().deregister_key(&self.key);
    }
}

impl<'owner, S, K, F> Stream for KeyHandle<'owner, S, K, F>
    where S: Stream,
          K: Copy + Eq + Hash,
          F: Fn(S::Item) -> (S::Item, K)
{
    type Item = S::Item;
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.owner.0.borrow_mut().poll_handle(self.key)
    }
}

struct Shared<S: Stream, K, F> {
    inner: S,
    key_fn: F,
    // The keys of all currently active handles.
    active_keys: HashSet<K>,
    current: Option<(S::Item, K)>,
    tasks: OrderMap<K, Task>,
    default_task: Option<Task>,
}

impl<S, K, F> Shared<S, K, F>
    where S: Stream,
          K: Eq + Hash
{
    fn new(inner: S, key_fn: F) -> Shared<S, K, F> {
        Shared {
            inner,
            key_fn,
            active_keys: HashSet::new(),
            current: None,
            tasks: OrderMap::new(),
            default_task: None,
        }
    }

    fn register_key(&mut self, key: K) -> bool {
        self.active_keys.insert(key)
    }

    fn deregister_key(&mut self, key: &K) {
        self.tasks.remove(key);
        self.current
            .take()
            .map(|(current_item, current_key)| {
                     if current_key == *key {
                         self.default_task.take().map(|default| default.notify());
                     }
                     self.current = Some((current_item, current_key));
                 });
        self.active_keys.remove(key);
    }
}

impl<S, K, F> Shared<S, K, F>
    where S: Stream,
          K: Copy + Eq + Hash,
          F: Fn(S::Item) -> (S::Item, K)
{
    fn poll_default(&mut self) -> Poll<Option<S::Item>, S::Error> {
        match self.current.take() {
            None => {
                // No item buffered, poll inner stream.
                match self.inner.poll() {
                    Ok(Async::Ready(Some(item))) => {
                        // Got new item, buffer it and call poll_default again.
                        let (item, key) = (self.key_fn)(item);
                        self.current = Some((item, key));
                        return self.poll_default();
                    }
                    Ok(Async::Ready(None)) => {
                        // End of underlying stream, notify all handles.
                        for (_, task) in self.tasks.drain((..)) {
                            task.notify();
                        }
                        return Ok(Async::Ready(None));
                    }
                    Ok(Async::NotReady) => {
                        // No item available, park.
                        self.default_task = Some(current());
                        return Ok(Async::NotReady);
                    }
                    Err(e) => return Err(e), // Error is simply propagated.
                }
            }

            Some((item, key)) => {
                // There's a buffered item + key.
                if self.active_keys.contains(&key) {
                    // There's a handle for this key, notify it if its blocking.
                    self.default_task = Some(current());
                    self.tasks.remove(&key).map(|task| task.notify());
                    self.current = Some((item, key));
                    return Ok(Async::NotReady);
                } else {
                    // No handle for this key, emit it on the owner.
                    // Also notify the next parked task.
                    self.tasks.pop().map(|(_, task)| task.notify());
                    return Ok(Async::Ready(Some(item)));
                }
            }
        }
    }

    fn poll_handle(&mut self, key: K) -> Poll<Option<S::Item>, S::Error> {
        match self.current.take() {
            None => {
                // No item buffered, poll inner stream.
                match self.inner.poll() {
                    Ok(Async::Ready(Some(item))) => {
                        // Got new item, buffer it and call poll_handle again.
                        let (item, item_key) = (self.key_fn)(item);
                        self.current = Some((item, item_key));
                        return self.poll_handle(key);
                    }
                    Ok(Async::Ready(None)) => {
                        // End of underlying stream, notify all handles.
                        for (_, task) in self.tasks.drain((..)) {
                            task.notify();
                        }
                        self.default_task.take().map(|default| default.notify());
                        return Ok(Async::Ready(None));
                    }
                    Ok(Async::NotReady) => {
                        // No item available, park.
                        self.tasks.insert(key, current());
                        return Ok(Async::NotReady);
                    }
                    Err(e) => return Err(e), // Error is simply propagated.
                }
            }

            Some((item, buffered_key)) => {
                // There's a buffered item + key.
                if buffered_key == key {
                    // We should emit the item, also notify the next parked task.
                    self.default_task
                        .take()
                        .map_or_else(|| { self.tasks.pop().map(|(_, task)| task.notify()); },
                                     |default| default.notify());

                    return Ok(Async::Ready(Some(item)));
                } else {
                    // Not our key, store item, park and let another task handle it.
                    self.tasks.insert(key, current());
                    self.current = Some((item, buffered_key));

                    self.tasks
                        .remove(&buffered_key)
                        .map_or_else(|| {
                                         self.default_task.take().map(|default| default.notify());
                                     },
                                     |task| task.notify());

                    return Ok(Async::NotReady);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::{Future, Stream, Sink};
    use futures::stream::iter_ok;

    use quickcheck::{QuickCheck, StdGen};
    use rand;
    use void::Void;
    use atm_async_utils::test_channel::*;
    use atm_async_utils::test_stream::*;

    #[test]
    fn test_success() {
        let rng = StdGen::new(rand::thread_rng(), 50);
        let mut quickcheck = QuickCheck::new().gen(rng).tests(1000);
        quickcheck.quickcheck(success as fn(usize) -> bool);
    }

    fn success(buf_size: usize) -> bool {
        let (sender, receiver) = test_channel::<u8, Void, Void>(buf_size + 1);

        let owner = OwnerMCS::new(receiver, |x| {
            (x,
             match x {
                 y if y % 3 == 0 => 1,
                 y if y % 5 == 0 => 2,
                 _ => 0,
             })
        });

        let default = owner.default_handle();
        let s1 = owner.key_handle(1);
        let s2 = owner.key_handle(2);

        let sending = sender.send_all(iter_ok::<_, Void>(0..20));

        let (_, threes, fives, defaults) = sending
            .join4(s1.collect(), s2.collect(), default.collect())
            .wait()
            .unwrap();

        return (threes == vec![0, 3, 6, 9, 12, 15, 18]) && (fives == vec![5, 10]) &&
               (defaults == vec![1, 2, 4, 7, 8, 11, 13, 14, 16, 17, 19]);
    }

    #[test]
    fn test_error() {
        let (sender, receiver) = test_channel::<u8, Void, u8>(8);
        let owner = OwnerMCS::new(TestStream::new(receiver,
                                                  vec![PollOp::Delegate,
                                                       PollOp::Err(13),
                                                       PollOp::Delegate,
                                                       PollOp::Delegate]),
                                  |x| (x, x));
        let mut r1 = owner.key_handle(1);
        let mut r2 = owner.key_handle(2);

        let sender = sender.send(1).wait().unwrap();
        let sender = sender.send(2).wait().unwrap();
        let _ = sender.send(1).wait().unwrap();

        assert!(r1.poll().unwrap().is_ready());
        assert!(r1.poll().is_err());
        assert!(r2.poll().unwrap().is_ready());
        assert!(r1.poll().unwrap().is_ready());
    }
}
