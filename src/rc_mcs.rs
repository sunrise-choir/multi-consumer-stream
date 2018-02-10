use std::cell::RefCell;
use std::hash::Hash;
use std::rc::Rc;

use futures::{Stream, Poll};

use shared::*;

/// A handle to a stream that receives all items without a corresponding `KeyMCS`.
pub struct DefaultMCS<S: Stream, K, F> {
    shared: Rc<RefCell<Shared<S, K, F>>>,
}

impl<S, K, F> DefaultMCS<S, K, F>
    where S: Stream,
          K: Copy + Eq + Hash,
          F: Fn(&S::Item) -> K
{
    /// Wrap a stream in a new `DefaultMCS`.
    pub fn new(stream: S, key_fn: F) -> DefaultMCS<S, K, F> {
        DefaultMCS { shared: Rc::new(RefCell::new(Shared::new(stream, key_fn))) }
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

impl<S, K, F> Stream for DefaultMCS<S, K, F>
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
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.shared.borrow_mut().poll_handle(self.key)
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

        let default = DefaultMCS::new(receiver, |x| match x {
            y if y % 3 == 0 => 1,
            y if y % 5 == 0 => 2,
            _ => 0,
        });

        let s1 = default.key_handle(1);
        let s2 = default.key_handle(2);

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
        let default = DefaultMCS::new(TestStream::new(receiver,
                                                      vec![PollOp::Delegate,
                                                           PollOp::Err(13),
                                                           PollOp::Delegate,
                                                           PollOp::Delegate]),
                                      |x| *x);
        let mut r1 = default.key_handle(1);
        let mut r2 = default.key_handle(2);

        let sender = sender.send(1).wait().unwrap();
        let sender = sender.send(2).wait().unwrap();
        let _ = sender.send(1).wait().unwrap();

        assert!(r1.poll().unwrap().is_ready());
        assert!(r1.poll().is_err());
        assert!(r2.poll().unwrap().is_ready());
        assert!(r1.poll().unwrap().is_ready());
    }
}
