use std::pin::Pin;
use std::ops::FnOnce;

use futures::prelude::*;
use futures::future::FusedFuture;
use std::task::{LocalWaker, Poll};

#[must_use = "futures do nothing unless polled"]
pub struct AndThenFut<Fut, F> {
    future: Fut,
    func: Option<F>,
}

impl<Fut, F> FusedFuture for AndThenFut<Fut, F> {
    fn is_terminated(&self) -> bool {
        self.func.is_none()
    }
}

impl<Fut, F, T> Future for AndThenFut<Fut, F>
    where Fut: TryFuture,
          F: FnOnce(Fut::Ok) -> std::result::Result<T, Fut::Error>,
{
    type Output = std::result::Result<T, Fut::Error>;

    fn poll(
        mut self: Pin<&mut Self>,
        lw: &LocalWaker,
    ) -> Poll<Self::Output> {
        match unsafe {
            self.as_mut().map_unchecked_mut(|f| &mut f.future)
        }.try_poll(lw) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                let op = unsafe { self.as_mut().get_unchecked_mut() }.func.take()
                    .expect("MapOk must not be polled after it returned `Poll::Ready`");
                Poll::Ready(result.and_then(op))
            }
        }
    }
}

pub trait TryFutureExt2: TryFuture {

    fn and_map<F, T>(self, f: F) -> AndThenFut<Self, F>
    where
        F: FnOnce(Self::Ok) -> std::result::Result<T, Self::Error>,
        Self: Sized
    {
        AndThenFut {
            future: self,
            func: Some(f)
        }
    }
}

impl<T> TryFutureExt2 for T where T: TryFuture {}