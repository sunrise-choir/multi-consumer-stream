//! TODO document
#![warn(missing_docs)]

extern crate futures_core;

use futures_core::{Poll, Stream};
use futures_core::task::Context;

// TODO split into sync and unsync implementations

pub struct MCS<S, Key, Fun, ErrFun> {
    s: S,
    key: Key,
    fun: Fun,
    err_fun: ErrFun,
}

impl<S, Key, Fun, ErrFun> MCS<S, Key, Fun, ErrFun>
    where S: Stream,
          Fun: Fn(&S::Item) -> Key,
          ErrFun: Fn(&S::Error) -> Key
{
    pub fn new(stream: S, key_fn: Fun) -> MCS<S, Key, Fun, ErrFun> {
        unimplemented!()
    }

    pub fn into_inner(self) -> S {
        unimplemented!()
    }

    pub fn key_handle(&self, key: Key) -> MCSHandle<S, Key, Fun, ErrFun> {
        unimplemented!()
    }

    pub fn try_key_handle(&self, key: Key) -> Option<MCSHandle<S, Key, Fun, ErrFun>> {
        unimplemented!()
    }
}

impl<S, Key, Fun, ErrFun> Stream for MCS<S, Key, Fun, ErrFun>
    where S: Stream,
          Fun: Fn(&S::Item) -> Key,
          ErrFun: Fn(&S::Error) -> Key
{
    type Item = S::Item;
    type Error = S::Error;

    fn poll_next(&mut self, cx: &mut Context) -> Poll<Option<Self::Item>, Self::Error> {
        unimplemented!()
    }
}

pub struct MCSHandle<S, Key, Fun, ErrFun> {
    s: S,
    key: Key,
    fun: Fun,
    err_fun: ErrFun,
}

impl<S, Key, Fun, ErrFun> Stream for MCSHandle<S, Key, Fun, ErrFun>
    where S: Stream,
          Fun: Fn(&S::Item) -> Key,
          ErrFun: Fn(&S::Error) -> Key
{
    type Item = S::Item;
    type Error = S::Error;

    fn poll_next(&mut self, cx: &mut Context) -> Poll<Option<Self::Item>, Self::Error> {
        unimplemented!()
    }
}
