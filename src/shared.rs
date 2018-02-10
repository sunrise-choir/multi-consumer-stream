use std::collections::HashSet;
use std::hash::Hash;

use futures::{Stream, Poll, Async};
use futures::task::{Task, current};
use ordermap::OrderMap;

pub struct Shared<S: Stream, K, F> {
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
    pub fn new(inner: S, key_fn: F) -> Shared<S, K, F> {
        Shared {
            inner,
            key_fn,
            active_keys: HashSet::new(),
            current: None,
            tasks: OrderMap::new(),
            default_task: None,
        }
    }

    pub fn register_key(&mut self, key: K) -> bool {
        self.active_keys.insert(key)
    }

    pub fn deregister_key(&mut self, key: &K) {
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
    pub fn poll_default(&mut self) -> Poll<Option<S::Item>, S::Error> {
        match self.current.take() {
            None => {
                // No item buffered, poll inner stream.
                match self.inner.poll() {
                    Ok(Async::Ready(Some(item))) => {
                        // Got new item, buffer it and call poll_default again.
                        let key = (self.key_fn)(&item);
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

    pub fn poll_handle(&mut self, key: K) -> Poll<Option<S::Item>, S::Error> {
        match self.current.take() {
            None => {
                // No item buffered, poll inner stream.
                match self.inner.poll() {
                    Ok(Async::Ready(Some(item))) => {
                        // Got new item, buffer it and call poll_handle again.
                        let item_key = (self.key_fn)(&item);
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
