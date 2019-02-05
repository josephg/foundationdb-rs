// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Implementations of the FDBDatabase C API
//!
//! https://apple.github.io/foundationdb/api-c.html#database

use std;
use std::sync::Arc;

use foundationdb_sys as fdb;
use futures::prelude::*;
use std::future::Future;

use crate::cluster::*;
use crate::error::{self, Error as FdbError, Result};
use crate::options;
use crate::transaction::*;


/// Represents a FoundationDB database — a mutable, lexicographically ordered mapping from binary keys to binary values.
///
/// Modifications to a database are performed via transactions.
#[derive(Clone)]
pub struct Database {
    // Order of fields should not be changed, because Rust drops field top-to-bottom (rfc1857), and
    // database should be dropped before cluster.
    inner: Arc<DatabaseInner>,
    cluster: Cluster,
}
impl Database {
    pub(crate) fn new(cluster: Cluster, db: *mut fdb::FDBDatabase) -> Self {
        let inner = Arc::new(DatabaseInner::new(db));
        Self { cluster, inner }
    }

    /// Called to set an option an on `Database`.
    pub fn set_option(&self, opt: options::DatabaseOption) -> Result<()> {
        unsafe { opt.apply(self.inner.inner) }
    }

    /// Creates a new transaction on the given database.
    pub fn create_trx(&self) -> Result<Transaction> {
        unsafe {
            let mut trx: *mut fdb::FDBTransaction = std::ptr::null_mut();
            error::eval(fdb::fdb_database_create_transaction(
                self.inner.inner,
                &mut trx as *mut _,
            ))?;
            Ok(Transaction::new(trx))
        }
    }

    /// `transact` returns a future which retries on error. It tries to resolve a future created by
    /// caller-provided function `f` inside a retry loop, providing it with a newly created
    /// transaction. After caller-provided future resolves, the transaction will be committed
    /// automatically.
    ///
    /// # Warning
    ///
    /// It might retry indefinitely if the transaction is highly contentious. It is recommended to
    /// set `TransactionOption::RetryLimit` or `TransactionOption::SetTimeout` on the transaction
    /// if the task need to be guaranteed to finish.
    #[must_use]
    pub fn transact<'a, Item, F, Fut>(
        &'a self,
        mut f: F,
    ) -> impl Future<Output=Result<Item>> + 'a
        where
            F: 'a + Fn(Transaction) -> Fut,
            Item: 'a,
            Fut: Future<Output=Result<Item>> + 'a,
    {
        async move {
            let trx = self.create_trx()?;

            loop {
                let result = await!(f(trx.clone()).and_then(|item| async {
                    await!(trx.commit())?;
                    Ok(item)
                }));

                match result {
                    Ok(v) => { return Ok(v) }
                    Err(e) => {
                        match trx.on_error(&e) {
                            // If the error isn't a FDB error, bail immediately.
                            // This is a bit nasty, but relatively straightforward.
                            None => { return Err(e) }
                            Some(fut) => {
                                if let Err(e) = await!(fut) { return Err(e) }
                            }
                        }
                        // Otherwise continue.
                    }
                }
            }
        }
    }
}

struct DatabaseInner {
    inner: *mut fdb::FDBDatabase,
}
impl DatabaseInner {
    fn new(inner: *mut fdb::FDBDatabase) -> Self {
        Self { inner }
    }
}
impl Drop for DatabaseInner {
    fn drop(&mut self) {
        unsafe {
            fdb::fdb_database_destroy(self.inner);
        }
    }
}
unsafe impl Send for DatabaseInner {}
unsafe impl Sync for DatabaseInner {}
