use std::pin::Pin;
use std::ptr::NonNull;
use std::ops::{FnOnce, Deref};
use std::fmt::Debug;

use futures::prelude::*;
use futures::future::FusedFuture;
use futures::task::{AtomicWaker, LocalWaker, Poll};

pub use crate::andthenfut::*;
use crate::error::{self, Error, Result};
use foundationdb_sys as fdb;

pub trait WaitFuture<T> = Future<Output=T> + Wait<T>;

/// A semi-safe wrapper for a foundationdb future object which is guaranteed to be valid (not destroyed).
struct FdbFutureInternal(NonNull<fdb::FDBFuture>);

impl FdbFutureInternal {
    fn new(ptr: *mut fdb::FDBFuture) -> Self {
        Self(NonNull::new(ptr).expect("future is null"))
    }

    fn as_mut_ptr(&self) -> *mut fdb::FDBFuture { self.0.as_ptr() }
}

impl Drop for FdbFutureInternal {
    fn drop(&mut self) {
        unsafe { fdb::fdb_future_destroy(self.0.as_ptr()) }
    }
}


/// A wrapped future which is guaranteed to be:
///
/// - Ready
/// - Not in an error state
/// - Valid (ie, not yet destroyed)
pub(crate) struct FdbFutureResult(FdbFutureInternal);



fn result_ok(_result: FdbFutureResult) -> Result<()> { Ok(()) }

/// An opaque type that represents a Future in the FoundationDB C API.
pub(crate) struct FdbFuture3 {
    f: Option<FdbFutureInternal>, // Set to None once the result has been returned

    // We need an AtomicWaker because FDB may resolve the promise in the context of the network thread.
    waker: AtomicWaker,
}


impl FdbFuture3 {
    pub(crate) unsafe fn new(fdb_fut: *mut fdb::FDBFuture) -> Self {
        FdbFuture3 {
            f: Some(FdbFutureInternal::new(fdb_fut)),
            waker: AtomicWaker::new(),
        }
    }

    pub(crate) unsafe fn new_mapped<F, R>(fdb_fut: *mut fdb::FDBFuture, f: F) -> impl Future<Output=Result<R>> + Wait<Result<R>>
            where F: FnOnce(FdbFutureResult) -> Result<R> {
        FdbFuture3::new(fdb_fut).and_map(f)
    }

    pub(crate) unsafe fn new_void(fdb_fut: *mut fdb::FDBFuture) -> impl Future<Output=Result<()>> + Wait<Result<()>> {
        FdbFuture3::new_mapped(fdb_fut, result_ok)
    }
}

impl Wait<Result<FdbFutureResult>> for FdbFuture3 {
    fn wait(mut self) -> Result<FdbFutureResult> {
        let f = self.f.take().expect("Cannot wait on polled FDB future");
        error::eval(unsafe { fdb::fdb_future_block_until_ready(f.as_mut_ptr()) })?;
        unsafe { FdbFutureResult::from_ready(f) }
    }
}

impl FusedFuture for FdbFuture3 {
    fn is_terminated(&self) -> bool { self.f.is_some() }
}

impl Future for FdbFuture3 {
    type Output = Result<FdbFutureResult>;

    fn poll(mut self: Pin<&mut Self>, lw: &LocalWaker) -> Poll<Self::Output> {
        let f_ptr = self.f.as_ref().expect("cannot poll after resolve").as_mut_ptr();

        if unsafe { fdb::fdb_future_is_ready(f_ptr) } == 0 {
            self.waker.register(lw);
            unsafe {
                let waker: *mut _ = &mut self.get_unchecked_mut().waker;
                fdb::fdb_future_set_callback(f_ptr, Some(fdb_future_callback), waker as *mut _);
            };
            return Poll::Pending;
        }

        // Alright; the promise *is* ready. Resolve.
        // Take out the future
        unsafe { Poll::Ready(FdbFutureResult::from_ready(self.get_mut().f.take().unwrap())) }
    }
}

extern "C" fn fdb_future_callback(
    _f: *mut fdb::FDBFuture,
    callback_parameter: *mut ::std::os::raw::c_void,
) {
    let waker: *mut AtomicWaker = callback_parameter as *mut _;
    unsafe { &*waker }.wake();
}



/// For output which is owned by the future (eg strings), we could either copy it into a Box or something, or keep it allocated alongside the future reference. Done this way avoids an allocation.
pub struct FutCell<'a, T: 'a> {
    _fut: FdbFutureResult,
    inner_value: T, // Actual lifetime attached to the FDB future lifetime.
    _dummy: std::marker::PhantomData<&'a T>
}

impl<'a, T> Deref for FutCell<'a, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.inner_value
    }
}

//impl<T> Deref for ScopedResult<&T> {
//    type Target = T;
//    fn deref(&self) -> &T {
//        self.inner_value
//    }
//}

impl<T: Clone> FutCell<'_, T> {
    pub fn into_owned(&self) -> T {
        self.inner_value.clone()
    }
}

impl<T: Debug> Debug for FutCell<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ScopedResult ({:?})", self.inner_value)
    }
}

/// Represents the output of fdb_future_get_keyvalue_array().
pub struct KeyValues<'a> {
    keyvalues: &'a [KeyValue<'a>],
    more: bool,
}

impl<'a> KeyValues<'a> {
    /// Returns true if (but not necessarily only if) values remain in the key range requested
    /// (possibly beyond the limits requested).
    pub(crate) fn more(&self) -> bool {
        self.more
    }
}

impl<'a> Deref for KeyValues<'a> {
    type Target = [KeyValue<'a>];

    fn deref(&self) -> &Self::Target {
        self.keyvalues
    }
}


/// Represents a single key-value pair in the output of fdb_future_get_keyvalue_array().
// Uses repr(packed) because c API uses 4-byte alignment for this struct
// TODO: field reordering might change a struct layout...
#[repr(packed)]
pub struct KeyValue<'a> {
    key: *const u8,
    key_len: u32,
    value: *const u8,
    value_len: u32,
    _dummy: std::marker::PhantomData<&'a u8>,
}
impl<'a> KeyValue<'a> {
    /// key
    pub fn key(&'a self) -> &'a [u8] {
        unsafe { std::slice::from_raw_parts(self.key, self.key_len as usize) }
    }
    /// value
    pub fn value(&'a self) -> &'a [u8] {
        unsafe { std::slice::from_raw_parts(self.value, self.value_len as usize) }
    }
}

impl FdbFutureResult {
    unsafe fn from_ready(fut: FdbFutureInternal) -> Result<Self> {
        // the future must be ready (unchecked)
        error::eval(unsafe { fdb::fdb_future_get_error(fut.as_mut_ptr()) })?;

        Ok(FdbFutureResult(fut))
    }

    fn into_scoped<'a, T: 'a>(self, value: T) -> FutCell<'a, T> {
        FutCell {
            _fut: self,
            inner_value: value,
            _dummy: std::marker::PhantomData,
        }
    }

    pub(crate) unsafe fn get_cluster(&self) -> Result<*mut fdb::FDBCluster> {
        let mut v: *mut fdb::FDBCluster = std::ptr::null_mut();
        error::eval(fdb::fdb_future_get_cluster(self.0.as_mut_ptr(), &mut v as *mut _))?;
        Ok(v)
    }

    pub(crate) unsafe fn get_database(&self) -> Result<*mut fdb::FDBDatabase> {
        let mut v: *mut fdb::FDBDatabase = std::ptr::null_mut();
        error::eval(fdb::fdb_future_get_database(self.0.as_mut_ptr(), &mut v as *mut _))?;
        Ok(v)
    }

    pub(crate) fn get_value<'a>(self) -> Result<Option<FutCell<'a, &'a [u8]>>> {
        let mut present = 0;
        let mut out_value = std::ptr::null();
        let mut out_len = 0;

        unsafe {
            error::eval(fdb::fdb_future_get_value(
                self.0.as_mut_ptr(),
                &mut present as *mut _,
                &mut out_value as *mut _,
                &mut out_len as *mut _,
            ))?
        }

        if present == 0 {
            return Ok(None);
        }

        // A value from `fdb_future_get_value` will alive until `fdb_future_destroy` is called and
        // `fdb_future_destroy` is called on `Self::drop`, so a lifetime of the value matches with
        // `self`
        let slice = unsafe { std::slice::from_raw_parts(out_value, out_len as usize) };
        Ok(Some(self.into_scoped(slice)))
    }

//    #[allow(unused)]
    pub(crate) fn get_key<'a>(self) -> Result<FutCell<'a, &'a [u8]>> {
        let mut out_value = std::ptr::null();
        let mut out_len = 0;

        unsafe {
            error::eval(fdb::fdb_future_get_key(
                self.0.as_mut_ptr(),
                &mut out_value as *mut _,
                &mut out_len as *mut _,
            ))?
        }

        // A value from `fdb_future_get_value` will alive until `fdb_future_destroy` is called and
        // `fdb_future_destroy` is called on `Self::drop`, so a lifetime of the value matches with
        // `self`
        let slice = unsafe { std::slice::from_raw_parts(out_value, out_len as usize) };
        Ok(self.into_scoped(slice))
    }

    pub(crate) fn get_string_array<'a>(self) -> Result<FutCell<'a, Vec<&'a [u8]>>> {
        use std::os::raw::c_char;

        let mut out_strings: *mut *const c_char = std::ptr::null_mut();
        let mut out_len = 0;

        unsafe {
            error::eval(fdb::fdb_future_get_string_array(
                self.0.as_mut_ptr(),
                &mut out_strings as *mut _,
                &mut out_len as *mut _,
            ))?
        }

        let out_len = out_len as usize;
        let out_strings: &[*const c_char] =
            unsafe { std::slice::from_raw_parts(out_strings, out_len) };

        let mut v = Vec::with_capacity(out_len);
        for i in 0..out_len {
            let cstr = unsafe { std::ffi::CStr::from_ptr(out_strings[i]) };
            v.push(cstr.to_bytes());
        }
        Ok(self.into_scoped(v))
    }

    pub(crate) fn get_keyvalue_array<'a>(self) -> Result<FutCell<'a, KeyValues<'a>>> {
        let mut out_keyvalues = std::ptr::null();
        let mut out_len = 0;
        let mut more = 0;

        unsafe {
            error::eval(fdb::fdb_future_get_keyvalue_array(
                self.0.as_mut_ptr(),
                &mut out_keyvalues as *mut _,
                &mut out_len as *mut _,
                &mut more as *mut _,
            ))?
        }

        let out_len = out_len as usize;
        let out_keyvalues: &[fdb::keyvalue] =
            unsafe { std::slice::from_raw_parts(out_keyvalues, out_len) };
        let out_keyvalues: &[KeyValue] = unsafe { std::mem::transmute(out_keyvalues) };
        Ok(self.into_scoped(KeyValues {
            keyvalues: out_keyvalues,
            more: (more != 0),
        }))
    }

    pub(crate) fn get_version(&self) -> Result<i64> {
        let mut version: i64 = 0;
        unsafe {
            error::eval(fdb::fdb_future_get_version(self.0.as_mut_ptr(), &mut version as *mut _))?;
        }
        Ok(version)
    }
}