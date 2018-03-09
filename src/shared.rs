// This is both the data shared between all handles to the same stream, and the logic shared between the sync and unsync versions.

use std::collections::HashSet;
use std::hash::Hash;

use indexmap::IndexMap;
use futures_core::{Stream, Poll, Async};
use futures_core::task::{Waker, Context};

pub struct Shared<S: Stream, Key, ItemFn, ErrFn> {
    pub inner: Option<S>,
    item_fn: ItemFn,
    err_fn: ErrFn,
    // The keys of all currently active handles.
    active_keys: HashSet<Key>,
    current: Option<(Result<S::Item, S::Error>, Key)>,
    wakers: IndexMap<Key, Waker>,
    default_waker: Option<Waker>,
    done: bool,
}

impl<S, Key, ItemFn, ErrFn> Shared<S, Key, ItemFn, ErrFn>
    where S: Stream,
          Key: Eq + Hash
{
    pub fn new(inner: S, item_fn: ItemFn, err_fn: ErrFn) -> Shared<S, Key, ItemFn, ErrFn> {
        Shared {
            inner: Some(inner),
            item_fn,
            err_fn,
            active_keys: HashSet::new(),
            current: None,
            wakers: IndexMap::new(),
            default_waker: None,
            done: false,
        }
    }

    pub fn register_key(&mut self, key: Key) -> bool {
        self.active_keys.insert(key)
    }

    pub fn deregister_key(&mut self, key: &Key) {
        self.wakers.remove(key);
        self.current
            .take()
            .map(|(current_result, current_key)| {
                     if current_key == *key {
                         self.default_waker.take().map(|default| default.wake());
                     }
                     self.current = Some((current_result, current_key));
                 });
        self.active_keys.remove(key);
    }
}

impl<S, Key, ItemFn, ErrFn> Shared<S, Key, ItemFn, ErrFn>
    where S: Stream,
          Key: Eq + Hash,
          ItemFn: Fn(&S::Item) -> Key,
          ErrFn: Fn(&S::Error) -> Key
{
    pub fn poll_default(&mut self, cx: &mut Context) -> Poll<Option<S::Item>, S::Error> {
        if self.done {
            self.wake_next_handle();
            Ok(Async::Ready(None))
        } else {
            match self.current.take() {
                None => {
                    let mut inner = self.inner.take().unwrap();
                    // No item buffered, poll inner stream.
                    match inner.poll_next(cx) {
                        Ok(Async::Ready(Some(item))) => {
                            // Got new item, buffer it and call poll_default again.
                            let key = (self.item_fn)(&item);
                            self.current = Some((Ok(item), key));
                            self.inner = Some(inner);
                            return self.poll_default(cx);
                        }
                        Err(err) => {
                            // Got new error, buffer it and call poll_default again.
                            let key = (self.err_fn)(&err);
                            self.current = Some((Err(err), key));
                            self.inner = Some(inner);
                            return self.poll_default(cx);
                        }
                        Ok(Async::Ready(None)) => {
                            self.done = true;
                            self.wake_next_handle();
                            self.inner = Some(inner);
                            Ok(Async::Ready(None))
                        }
                        Ok(Async::Pending) => {
                            // No item available, park.
                            self.default_waker = Some(cx.waker());
                            self.inner = Some(inner);
                            Ok(Async::Pending)
                        }
                    }
                }

                Some((result, key)) => {
                    // There's a buffered result + key.
                    if self.active_keys.contains(&key) {
                        // There's a handle for this key, notify it if its blocking.
                        self.default_waker = Some(cx.waker());
                        self.wakers.remove(&key).map(|waker| waker.wake());
                        self.current = Some((result, key));
                        Ok(Async::Pending)
                    } else {
                        // No handle for this key, emit/yield it.
                        // // Also notify the next parked task.
                        self.wake_next_handle(); // TODO rename notify stuff to wake
                        match result {
                            Ok(item) => Ok(Async::Ready(Some(item))),
                            Err(err) => Err(err),
                        }
                    }
                }
            }
        }
    }

    pub fn poll_handle(&mut self, key: Key, cx: &mut Context) -> Poll<Option<S::Item>, S::Error> {
        if self.done {
            self.wake_next_handle();
            Ok(Async::Ready(None))
        } else {
            match self.current.take() {
                None => {
                    let mut inner = self.inner.take().expect("Polled key handle after calling into_inner on the default handle");
                    // No item buffered, poll inner stream.
                    match inner.poll_next(cx) {
                        Ok(Async::Ready(Some(item))) => {
                            // Got new item, buffer it and call poll_handle again.
                            let item_key = (self.item_fn)(&item);
                            self.current = Some((Ok(item), item_key));
                            self.inner = Some(inner);
                            self.poll_handle(key, cx)
                        }
                        Err(err) => {
                            // Got new error, buffer it and call poll_handle again.
                            let err_key = (self.err_fn)(&err);
                            self.current = Some((Err(err), err_key));
                            self.inner = Some(inner);
                            return self.poll_handle(key, cx);
                        }
                        Ok(Async::Ready(None)) => {
                            // End of underlying stream.
                            self.done = true;
                            self.wake_default_or_next();
                            self.inner = Some(inner);
                            Ok(Async::Ready(None))
                        }
                        Ok(Async::Pending) => {
                            // No item available, park.
                            self.wakers.insert(key, cx.waker());
                            self.inner = Some(inner);
                            Ok(Async::Pending)
                        }
                    }
                }

                Some((result, buffered_key)) => {
                    // There's a buffered result + key.
                    if buffered_key == key {
                        // We should emit the item, also notify the next parked task.
                        self.wake_default_or_next();
                        match result {
                            Ok(item) => Ok(Async::Ready(Some(item))),
                            Err(err) => Err(err),
                        }
                    } else {
                        // Not our key, store item, park and let another task handle it.
                        self.wakers
                            .remove(&buffered_key)
                            .map_or_else(|| self.wake_default(), |waker| waker.wake());

                        self.wakers.insert(key, cx.waker());
                        self.current = Some((result, buffered_key));

                        Ok(Async::Pending)
                    }
                }
            }
        }
    }

    // wake the next key handle
    fn wake_next_handle(&mut self) {
        self.wakers.pop().map(|(_, waker)| waker.wake());
    }

    // wake the default handle
    fn wake_default(&mut self) {
        self.default_waker.take().map(|default| default.wake());
    }

    // wake the default handle or the next key handle if default can't be woken
    fn wake_default_or_next(&mut self) {
        self.default_waker
            .take()
            .map_or_else(|| self.wake_next_handle(), |default| default.wake());
    }
}
