use std::pin::Pin;
use std::ops::FnOnce;
use std::result::Result;

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

impl<Fut, F, R> Future for AndThenFut<Fut, F>
    where Fut: TryFuture,
          F: FnOnce(Fut::Ok) -> Result<R, Fut::Error>,
{
    type Output = Result<R, Fut::Error>;

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
                    .expect("AndThenFut must not be polled after it returned `Poll::Ready`");
                Poll::Ready(result.and_then(op))
            }
        }
    }
}

pub trait TryFutureExt2: TryFuture {

    fn and_map<F, T>(self, f: F) -> AndThenFut<Self, F>
    where
        F: FnOnce(Self::Ok) -> Result<T, Self::Error>,
        Self: Sized
    {
        AndThenFut {
            future: self,
            func: Some(f)
        }
    }
}

impl<T> TryFutureExt2 for T where T: TryFuture {}


/// Block waiting for a value to be ready
pub trait Wait<R> {
    fn wait(self) -> R;
}

impl<Fut, F, Mid, Out, Err> Wait<Result<Out, Err>> for AndThenFut<Fut, F>
    where Fut: TryFuture<Ok=Mid, Error=Err> + Wait<Result<Mid, Err>>,
          F: FnOnce(Mid) -> Result<Out, Err>
{
    fn wait(mut self) -> Result<Out, Err> {
        let r1 = self.future.wait()?;
        let op = self.func.take().expect("AndThenFut cannot be polled and waited");
        op(r1)
    }
}