use std::cell::RefCell;
use std::hash::Hash;

use futures::{Stream, Poll};

use shared::*;

/// A wrapper around a stream, that allows obtaining multiple handles for
/// reading from that stream.
///
/// A `KeyHandle` only receives items for which the `key_fn` returns a
/// certain key. All other items are emitted on a `DefaultHandle`.
pub struct OwnerMCS<S: Stream, K, F>(RefCell<Shared<S, K, F>>);

impl<S, K, F> OwnerMCS<S, K, F>
    where S: Stream,
          K: Copy + Eq + Hash,
          F: Fn(&S::Item) -> K
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
          F: Fn(&S::Item) -> K
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
          F: Fn(&S::Item) -> K
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
          F: Fn(&S::Item) -> K
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
          F: Fn(&S::Item) -> K
{
    type Item = S::Item;
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.owner.0.borrow_mut().poll_handle(self.key)
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

        let owner = OwnerMCS::new(receiver, |x| match x {
            y if y % 3 == 0 => 1,
            y if y % 5 == 0 => 2,
            _ => 0,
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
                                  |x| *x);
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
