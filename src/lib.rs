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
use std::rc::Rc;

use futures::{Stream, Poll, Async};
use futures::task::{Task, current};
use ordermap::OrderMap;

/// Create a new `MCS`, wrapping the given stream.
pub fn mcs<S: Stream, K: Copy + Eq + Hash, F: Fn(&S::Item) -> K>(stream: S,
                                                                 key_fn: F)
                                                                 -> MCS<S, K, F> {
    MCS { shared: Rc::new(RefCell::new(Shared::new(stream, key_fn))) }
}

/// A handle to a stream that receives all items without a corresponding `KeyMCS`.
///
/// If one of the key handles triggers an error, the error is emitted on the main
/// `MCS`. Further calls to `poll` after an error has been emitted panic.
pub struct MCS<S: Stream, K, F> {
    shared: Rc<RefCell<Shared<S, K, F>>>,
}

impl<S, K, F> MCS<S, K, F>
    where S: Stream,
          K: Copy + Eq + Hash,
          F: Fn(&S::Item) -> K
{
    /// Create a `KeyMCS` handle to the underlying stream. The handle receives all
    /// items for which the `key_fn` returns `key`.
    ///
    /// Panics if there is already a handle for that key.
    pub fn key_handle(&self, key: K) -> KeyMCS<S, K, F> {
        KeyMCS::new(self.shared.clone(), key)
    }

    /// Create a `KeyMCS` handle to the underlying stream. The handle receives all
    /// items for which the `key_fn` returns `key`. This returns `None` if there
    /// is already a handle for the given key.
    pub fn try_key_handle(&self, key: K) -> Option<KeyMCS<S, K, F>> {
        KeyMCS::try_new(self.shared.clone(), key)
    }

    /// Consume the `MCS` and return ownership of the wrapped stream. If the
    /// stream did not terminate before calling this, subsequent calls to `poll`
    /// on a `KeyMCS` will panic.
    pub fn into_inner(self) -> S {
        self.shared.borrow_mut().inner.take().unwrap()
    }
}

impl<S, K, F> Stream for MCS<S, K, F>
    where S: Stream,
          K: Copy + Eq + Hash,
          F: Fn(&S::Item) -> K
{
    type Item = S::Item;
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.shared.borrow_mut().poll_default()
    }
}

/// A handle to a stream that receives all items for which the `key_fn` returns
/// the value associated with this `KeyMCS`.
///
/// All errors are emitted on the main `MCS`. A `KeyMCS` just signals errors by
/// emitting `Err(())`.
pub struct KeyMCS<S: Stream, K: Eq + Hash, F> {
    shared: Rc<RefCell<Shared<S, K, F>>>,
    key: K,
}

impl<S: Stream, K, F> KeyMCS<S, K, F>
    where S: Stream,
          K: Copy + Eq + Hash,
          F: Fn(&S::Item) -> K
{
    fn new(shared: Rc<RefCell<Shared<S, K, F>>>, key: K) -> KeyMCS<S, K, F> {
        assert!(shared.borrow_mut().register_key(key),
                "Tried to register duplicate handles");
        KeyMCS { shared, key }
    }

    fn try_new(shared: Rc<RefCell<Shared<S, K, F>>>, key: K) -> Option<KeyMCS<S, K, F>> {
        if shared.borrow_mut().register_key(key) {
            Some(KeyMCS { shared, key })
        } else {
            None
        }
    }

    /// Create a `KeyMCS` handle to the underlying stream. The handle receives all
    /// items for which the `key_fn` returns `key`.
    ///
    /// Panics if there is already a handle for that key.
    pub fn key_handle(&self, key: K) -> KeyMCS<S, K, F> {
        KeyMCS::new(self.shared.clone(), key)
    }

    /// Create a `KeyMCS` handle to the underlying stream. The handle receives all
    /// items for which the `key_fn` returns `key`. This returns `None` if there
    /// is already a handle for the given key.
    pub fn try_key_handle(&self, key: K) -> Option<KeyMCS<S, K, F>> {
        KeyMCS::try_new(self.shared.clone(), key)
    }
}

impl<S, K, F> Drop for KeyMCS<S, K, F>
    where S: Stream,
          K: Eq + Hash
{
    /// Deregisters the key of this `KeyMCS`.
    fn drop(&mut self) {
        self.shared.borrow_mut().deregister_key(&self.key);
    }
}

impl<S, K, F> Stream for KeyMCS<S, K, F>
    where S: Stream,
          K: Copy + Eq + Hash,
          F: Fn(&S::Item) -> K
{
    type Item = S::Item;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.shared.borrow_mut().poll_handle(self.key)
    }
}

enum StreamState {
    Active,
    Done,
    Errored,
}

struct Shared<S: Stream, K, F> {
    inner: Option<S>,
    key_fn: F,
    // The keys of all currently active handles.
    active_keys: HashSet<K>,
    current: Option<(S::Item, K)>,
    tasks: OrderMap<K, Task>,
    default_task: Option<Task>,
    state: StreamState,
    error: Option<S::Error>,
}

impl<S, K, F> Shared<S, K, F>
    where S: Stream,
          K: Eq + Hash
{
    fn new(inner: S, key_fn: F) -> Shared<S, K, F> {
        Shared {
            inner: Some(inner),
            key_fn,
            active_keys: HashSet::new(),
            current: None,
            tasks: OrderMap::new(),
            default_task: None,
            state: StreamState::Active,
            error: None,
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
          F: Fn(&S::Item) -> K
{
    fn poll_default(&mut self) -> Poll<Option<S::Item>, S::Error> {
        match self.state {
            StreamState::Done => {
                self.notify_next_handle();
                Ok(Async::Ready(None))
            }

            StreamState::Errored => {
                self.notify_next_handle();
                Err(self.error
                        .take()
                        .expect("Polled MCS after it yielded an error"))
            }

            StreamState::Active => {
                match self.current.take() {
                    None => {
                        let mut inner = self.inner.take().unwrap();
                        // No item buffered, poll inner stream.
                        match inner.poll() {
                            Ok(Async::Ready(Some(item))) => {
                                // Got new item, buffer it and call poll_default again.
                                let key = (self.key_fn)(&item);
                                self.current = Some((item, key));
                                self.inner = Some(inner);
                                return self.poll_default();
                            }
                            Ok(Async::Ready(None)) => {
                                self.state = StreamState::Done;
                                self.notify_next_handle();
                                self.inner = Some(inner);
                                Ok(Async::Ready(None))
                            }
                            Ok(Async::NotReady) => {
                                // No item available, park.
                                self.default_task = Some(current());
                                self.inner = Some(inner);
                                Ok(Async::NotReady)
                            }
                            Err(err) => {
                                self.state = StreamState::Errored;
                                self.notify_next_handle();
                                self.inner = Some(inner);
                                Err(err)
                            }
                        }
                    }

                    Some((item, key)) => {
                        // There's a buffered item + key.
                        if self.active_keys.contains(&key) {
                            // There's a handle for this key, notify it if its blocking.
                            self.default_task = Some(current());
                            self.tasks.remove(&key).map(|task| task.notify());
                            self.current = Some((item, key));
                            Ok(Async::NotReady)
                        } else {
                            // No handle for this key, emit it.
                            // Also notify the next parked task.
                            self.notify_next_handle();
                            Ok(Async::Ready(Some(item)))
                        }
                    }
                }
            }
        }
    }

    fn poll_handle(&mut self, key: K) -> Poll<Option<S::Item>, ()> {
        match self.state {
            StreamState::Done => {
                self.notify_next_handle();
                Ok(Async::Ready(None))
            }

            StreamState::Errored => {
                self.notify_next_handle();
                Err(())
            }

            StreamState::Active => {
                match self.current.take() {
                    None => {
                        let mut inner = self.inner.take().expect("Polled key handle after calling into_inner on the default handle");
                        // No item buffered, poll inner stream.
                        match inner.poll() {
                            Ok(Async::Ready(Some(item))) => {
                                // Got new item, buffer it and call poll_handle again.
                                let item_key = (self.key_fn)(&item);
                                self.current = Some((item, item_key));
                                self.inner = Some(inner);
                                self.poll_handle(key)
                            }
                            Ok(Async::Ready(None)) => {
                                // End of underlying stream.
                                self.notify_default_or_next();
                                self.state = StreamState::Done;
                                self.inner = Some(inner);
                                Ok(Async::Ready(None))
                            }
                            Ok(Async::NotReady) => {
                                // No item available, park.
                                self.tasks.insert(key, current());
                                self.inner = Some(inner);
                                Ok(Async::NotReady)
                            }
                            Err(err) => {
                                self.state = StreamState::Errored;
                                self.error = Some(err);
                                self.notify_default_or_next();
                                self.inner = Some(inner);
                                Err(())
                            }
                        }
                    }

                    Some((item, buffered_key)) => {
                        // There's a buffered item + key.
                        if buffered_key == key {
                            // We should emit the item, also notify the next parked task.
                            self.notify_default_or_next();
                            Ok(Async::Ready(Some(item)))
                        } else {
                            // Not our key, store item, park and let another task handle it.
                            self.tasks.insert(key, current());
                            self.current = Some((item, buffered_key));

                            self.tasks
                                .remove(&buffered_key)
                                .map_or_else(|| self.notify_default(), |task| task.notify());

                            Ok(Async::NotReady)
                        }
                    }
                }
            }
        }
    }

    // notify the next key handle
    fn notify_next_handle(&mut self) {
        self.tasks.pop().map(|(_, task)| task.notify());
    }

    // notify the default handle
    fn notify_default(&mut self) {
        self.default_task.take().map(|default| default.notify());
    }

    // notify the default handle or the next key handle if default can't be notified
    fn notify_default_or_next(&mut self) {
        self.default_task
            .take()
            .map_or_else(|| self.notify_next_handle(), |default| default.notify());
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    use futures::{Future, Stream, Sink};
    use futures::stream::iter_ok;
    use futures::future::ok;

    use quickcheck::{QuickCheck, StdGen};
    use rand;
    use atm_async_utils::test_channel::*;
    use atm_async_utils::test_stream::*;

    #[test]
    fn test_success() {
        let rng = StdGen::new(rand::thread_rng(), 50);
        let mut quickcheck = QuickCheck::new().gen(rng).tests(1000);
        quickcheck.quickcheck(success as fn(usize) -> bool);
    }

    fn success(buf_size: usize) -> bool {
        let (sender, receiver) = test_channel::<u8, (), ()>(buf_size + 1);

        let default = mcs(receiver, |x| match x {
            y if y % 3 == 0 => 1,
            y if y % 5 == 0 => 2,
            _ => 0,
        });

        let s1 = default.key_handle(1);
        let s2 = default.key_handle(2);

        let sending = sender.send_all(iter_ok::<_, ()>(0..20));

        let (_, threes, fives, defaults) = sending
            .join4(s1.collect(),
                   s2.collect(),
                   default.map_err(|_| ()).collect())
            .wait()
            .unwrap();

        return (threes == vec![0, 3, 6, 9, 12, 15, 18]) && (fives == vec![5, 10]) &&
               (defaults == vec![1, 2, 4, 7, 8, 11, 13, 14, 16, 17, 19]);
    }

    #[test]
    fn test_error() {
        let (sender, receiver) = test_channel::<bool, bool, u8>(8);
        let default = mcs(TestStream::new(receiver,
                                          vec![PollOp::Delegate,
                                               PollOp::Err(13),
                                               PollOp::Delegate,
                                               PollOp::Delegate]),
                          |x| *x);
        let r1 = default.key_handle(true);
        let r2 = default.key_handle(false);

        let sending = sender
            .send_all(iter_ok::<_, bool>(0..8).map(|_| false))
            .map(|_| true);

        let default = default
            .for_each(|_| ok(()))
            .map(|_| false)
            .or_else(|err| ok(err == 13));
        let r1 = r1.for_each(|_| ok(()))
            .map(|_| false)
            .or_else(|err| ok(err == ()));
        let r2 = r2.for_each(|_| ok(()))
            .map(|_| false)
            .or_else(|err| ok(err == ()));
        let receiving = default.join3(r1, r2);

        let (_, (worked0, worked1, worked2)) = sending.join(receiving).wait().unwrap();
        assert!(worked0 && worked1 && worked2);
    }
}
