// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Implementations of the FDBCluster C API
//!
//! https://apple.github.io/foundationdb/api-c.html#cluster

use foundationdb_sys as fdb;
use std::future::Future;
use crate::future::*;
use std;
use std::sync::Arc;

use crate::database::*;
use crate::error::*;

/// An opaque type that represents a Cluster in the FoundationDB C API.
#[derive(Clone)]
pub struct Cluster(Arc<ClusterInner>);

impl Cluster {
    /// Returns an FdbFuture which will be set to an FDBCluster object.
    ///
    /// # Arguments
    ///
    /// * `path` - A string giving a local path of a cluster file (often called ‘fdb.cluster’) which contains connection information for the FoundationDB cluster. See `foundationdb::default_config_path()`
    ///
    /// TODO: implement Default for Cluster where: If cluster_file_path is NULL or an empty string, then a default cluster file will be used. see
    pub fn new(path: &str) -> impl WaitFuture<Result<Cluster>> {
        let path_str = std::ffi::CString::new(path).unwrap();
        unsafe {
            let f = fdb::fdb_create_cluster(path_str.as_ptr());
            FdbFuture3::new_mapped(f, |r| {
                Ok(Cluster(Arc::new(ClusterInner(r.get_cluster()?))))
            })
        }
    }

    // TODO: fdb_cluster_set_option impl

    /// Returns an `FdbFuture` which will be set to an `Database` object.
    ///
    /// TODO: impl Future
    pub fn create_database(&self) -> impl WaitFuture<Result<Database>> {
        unsafe {
            let f_db = fdb::fdb_cluster_create_database((self.0).0, b"DB" as *const _, 2);
            let cluster = self.clone();
            FdbFuture3::new_mapped(f_db, |r| Ok(Database::new(cluster, r.get_database()?)))
        }
    }
}

//TODO: should check if `fdb::FDBCluster` is thread-safe.
struct ClusterInner(*mut fdb::FDBCluster);
impl Drop for ClusterInner {
    fn drop(&mut self) {
        unsafe {
            fdb::fdb_cluster_destroy(self.0);
        }
    }
}
unsafe impl Send for ClusterInner {}
unsafe impl Sync for ClusterInner {}
