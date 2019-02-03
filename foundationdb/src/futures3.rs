use std::pin::Pin;
use std::ptr::NonNull;
use std::ops::FnOnce;

use futures_preview::prelude::*;
use futures_preview::task::{AtomicWaker, LocalWaker, Poll};

use crate::error::{Error, Result};
use foundationdb_sys as fdb;

/// An opaque type that represents a Future in the FoundationDB C API.
pub(crate) struct FdbFuture3 {
    f: NonNull<fdb::FDBFuture>, // Set to None once the value is extracted

    // We need an AtomicWaker because FDB may resolve the promise in the context of the network thread.
    waker: AtomicWaker,
}

impl FdbFuture3 {
    pub(crate) fn new(fdb_fut: *mut fdb::FDBFuture) -> Self {
        FdbFuture3 {
            f: NonNull::new(fdb_fut).expect("future is null"),
            waker: AtomicWaker::new(),
        }
    }

    pub(crate) fn new_mapped<T, F>(fdb_fut: *mut fdb::FDBFuture, f: F) -> impl TryFuture<Ok=T, Error=Error>
        where F: FnOnce(NonNull<fdb::FDBFuture>) -> T {

        FdbFuture3::new(fdb_fut).map_ok(|f_result| f(f_result.0))
    }
}


/// A wrapped future which is guaranteed to be:
///
/// - Ready
/// - Not in an error state
/// - Valid (ie, not yet destroyed)
pub(crate) struct FdbFutureResult(NonNull<fdb::FDBFuture>);

impl TryFuture for FdbFuture3 {

    type Ok = FdbFutureResult;
    type Error = Error;

    fn try_poll(self: Pin<&mut Self>, lw: &LocalWaker) -> Poll<Result<Self::Ok>> {
        let f_ptr = self.f.as_ptr();

        if unsafe {fdb::fdb_future_is_ready(f_ptr)} == 0 {
            self.waker.register(lw);
            unsafe {
                let waker: *mut _ = &mut self.get_unchecked_mut().waker;
                fdb::fdb_future_set_callback(f_ptr, Some(fdb_future_callback), waker as *mut _);
            };
            return Poll::Pending;
        }

        // Alright; the promise *is* ready. Resolve.
        let err_code = unsafe { fdb::fdb_future_get_error(f_ptr) };
        if err_code != 0 { return Poll::Ready(Err(Error::from(err_code))); }

        Poll::Ready(Ok(FdbFutureResult(self.f)))
    }
}

extern "C" fn fdb_future_callback(
    _f: *mut fdb::FDBFuture,
    callback_parameter: *mut ::std::os::raw::c_void,
) {
    let waker: *mut AtomicWaker = callback_parameter as *mut _;
    unsafe { &*waker }.wake();
}
