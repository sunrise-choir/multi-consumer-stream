//! TODO document
#![warn(missing_docs)]

// TODO AGPL

extern crate futures_core;
extern crate indexmap;

mod shared;

use futures_core::{Poll, Stream};
use futures_core::task::Context;

// TODO split into sync and unsync implementations

pub struct MCS<S, Key, ItemFn, ErrFn> {
    s: S,
    key: Key,
    item_fn: ItemFn,
    err_fn: ErrFn,
}

impl<S, Key, ItemFn, ErrFn> MCS<S, Key, ItemFn, ErrFn>
    where S: Stream,
          ItemFn: Fn(&S::Item) -> Key,
          ErrFn: Fn(&S::Error) -> Key
{
    pub fn new(stream: S, item_fn: ItemFn, err_fn: ErrFn) -> MCS<S, Key, ItemFn, ErrFn> {
        unimplemented!()
    }

    pub fn into_inner(self) -> S {
        unimplemented!()
    }

    pub fn key_handle(&self, key: Key) -> MCSHandle<S, Key, ItemFn, ErrFn> {
        unimplemented!()
    }

    pub fn try_key_handle(&self, key: Key) -> Option<MCSHandle<S, Key, ItemFn, ErrFn>> {
        unimplemented!()
    }
}

impl<S, Key, ItemFn, ErrFn> Stream for MCS<S, Key, ItemFn, ErrFn>
    where S: Stream,
          ItemFn: Fn(&S::Item) -> Key,
          ErrFn: Fn(&S::Error) -> Key
{
    type Item = S::Item;
    type Error = S::Error;

    fn poll_next(&mut self, cx: &mut Context) -> Poll<Option<Self::Item>, Self::Error> {
        unimplemented!()
    }
}

pub struct MCSHandle<S, Key, ItemFn, ErrFn> {
    s: S,
    key: Key,
    item_fn: ItemFn,
    err_fn: ErrFn,
}

impl<S, Key, ItemFn, ErrFn> Stream for MCSHandle<S, Key, ItemFn, ErrFn>
    where S: Stream,
          ItemFn: Fn(&S::Item) -> Key,
          ErrFn: Fn(&S::Error) -> Key
{
    type Item = S::Item;
    type Error = S::Error;

    fn poll_next(&mut self, cx: &mut Context) -> Poll<Option<Self::Item>, Self::Error> {
        unimplemented!()
    }
}

// TODO destructors

// TODO tests

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_name() {
        unimplemented!()
    }
}
