// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

#![feature(futures_api, async_await, await_macro)]

extern crate foundationdb;
//extern crate futures;
#[macro_use]
extern crate lazy_static;

use std::future::*;
use foundationdb::future::Wait;

use foundationdb::*;
use futures::*;
use futures::executor::*;

mod common;

#[test]
fn test_set_get() -> Result<(), Error> {
    common::setup_static();
    let cluster = Cluster::new(foundationdb::default_config_path()).wait()?;
    let db = cluster.create_database().wait()?;

    block_on(async || -> Result<(), Error> {

        await!(db.transact(async move |tr| {
            tr.set(b"hello", b"world");
            Ok(())
        }))?;

        let result = await!(db.transact(|tr| tr.get(b"hello", false)))?;

        assert_eq!(*result.unwrap(), b"world");

        await!(db.transact(async move |tr| {
            tr.clear(b"hello");
            Ok(())
        }))?;

        let result = await!(db.transact(|tr| {
            tr.get(b"hello", false)
        }))?;

        assert!(result.is_none());
        Ok(())
    }())?;

    Ok(())
}

#[test]
fn test_get_multi() -> Result<(), Error> {
    common::setup_static();
    let cluster = Cluster::new(foundationdb::default_config_path()).wait()?;
    let db = cluster.create_database().wait()?;

    block_on(async || -> Result<(), Error> {
//        await!(db.transact(async move |tr| {
//            tr.set(b"hello", b"world");
//            tr.set(b"bar", b"blat");
//            Ok(())
//        }))?;

        let keys:&[&[u8]] = &[b"hello", b"world", b"foo", b"bar"];
        let result = await!(db.transact(async move |tr| {
            let r1 = await!(futures::future::join_all(keys.iter().map(|k| tr.get(k, false))));
            r1.into_iter().collect::<Result<Vec<_>, _>>()
        }))?;

        eprintln!("res {:?}", result);

        Ok(())
    }())
}

#[test]
fn test_set_conflict() -> Result<(), Error> {
    common::setup_static();
    let cluster = Cluster::new(foundationdb::default_config_path()).wait()?;
    let db = cluster.create_database().wait()?;

    let key = b"test-conflict";

    block_on(async || -> Result<(), Error> {
        let trx2 = db.create_trx()?;
        await!(trx2.get(key, false))?;

        // Commit concurrent transaction to create conflict.
        await!(db.transact(async move |trx1| {
            trx1.set(key, common::random_str(10).as_bytes());
            Ok(())
        }))?;

        trx2.set(key, common::random_str(10).as_bytes());
        let r = await!(trx2.commit());

//        println!("{:?}", r);
        // 1020 == "Transaction not committed due to conflict with another transaction"
        assert_eq!(r.err().expect("Transaction should have conflicted").code().unwrap().get(), 1020);
        Ok(())
    }())
}

//
//#[test]
//fn test_set_conflict_snapshot() {
//    common::setup_static();
//
//    let key = b"test-conflict-snapshot";
//    let fut = Cluster::new(foundationdb::default_config_path())
//        .and_then(|cluster| cluster.create_database())
//        .and_then(|db| {
//            // First transaction. It will be committed before second one.
//            let fut_set1 = result(db.create_trx()).and_then(|trx1| {
//                trx1.set(key, common::random_str(10).as_bytes());
//                trx1.commit()
//            });
//
//            // Second transaction.
//            result(db.create_trx())
//                .and_then(|trx2| {
//                    // snapshot read does not set conflict range, so both transaction will be
//                    // committed.
//                    trx2.get(key, true)
//                })
//                .and_then(move |val| {
//                    // commit first transaction
//                    fut_set1.map(move |_trx1| val.transaction())
//                })
//                .and_then(|trx2| {
//                    // commit seconds transaction, which will *not* cause conflict because of
//                    // snapshot read
//                    trx2.set(key, common::random_str(10).as_bytes());
//                    trx2.commit()
//                })
//                .map(|_v| ())
//        });
//
//    fut.wait().expect("failed to run")
//}
//
//// Makes the key dirty. It will abort transactions which performs non-snapshot read on the `key`.
//fn make_dirty(db: &Database, key: &[u8]) {
//    let trx = db.create_trx().unwrap();
//    trx.set(key, b"");
//    trx.commit().wait().unwrap();
//}
//
//#[test]
//fn test_transact() {
//    use std::sync::{atomic::*, Arc};
//
//    const KEY: &[u8] = b"test-transact";
//    const RETRY_COUNT: usize = 5;
//    common::setup_static();
//
//    let try_count = Arc::new(AtomicUsize::new(0));
//    let try_count0 = try_count.clone();
//
//    let fut = Cluster::new(foundationdb::default_config_path())
//        .and_then(|cluster| cluster.create_database())
//        .and_then(|db| {
//            // start tranasction with retry
//            db.transact(move |trx| {
//                // increment try counter
//                try_count0.fetch_add(1, Ordering::SeqCst);
//
//                trx.set_option(options::TransactionOption::RetryLimit(RETRY_COUNT as u32))
//                    .expect("failed to set retry limit");
//
//                let db = trx.database();
//
//                // update conflict range
//                trx.get(KEY, false).and_then(move |res| {
//                    // make current transaction invalid by making conflict
//                    make_dirty(&db, KEY);
//
//                    let trx = res.transaction();
//                    trx.set(KEY, common::random_str(10).as_bytes());
//                    // `Database::transact` will handle commit by itself, so returns without commit
//                    Ok(())
//                })
//            }).then(|res| match res {
//                Ok(_) => panic!("should not be able to commit"),
//                Err(e) => {
//                    eprintln!("failed as expected: {:?}", e);
//                    Ok(())
//                }
//            })
//        });
//
//    fut.wait().expect("failed to run");
//    // `TransactionOption::RetryCount` does not count first try, so `try_count` should be equal to
//    // `RETRY_COUNT+1`
//    assert_eq!(try_count.load(Ordering::SeqCst), RETRY_COUNT + 1);
//}
//
//#[test]
//fn test_versionstamp() {
//    const KEY: &[u8] = b"test-versionstamp";
//    common::setup_static();
//
//    let fut = Cluster::new(foundationdb::default_config_path())
//        .and_then(|cluster| cluster.create_database())
//        .and_then(|db| result(db.create_trx()))
//        .and_then(|trx| {
//            trx.set(KEY, common::random_str(10).as_bytes());
//            let f_version = trx.get_versionstamp();
//            trx.commit().and_then(move |_trx| f_version)
//        })
//        .map(|r| {
//            eprintln!("versionstamp: {:?}", r.versionstamp());
//        });
//
//    fut.wait().expect("failed to run");
//}
//
//#[test]
//fn test_read_version() {
//    common::setup_static();
//
//    let fut = Cluster::new(foundationdb::default_config_path())
//        .and_then(|cluster| cluster.create_database())
//        .and_then(|db| result(db.create_trx()))
//        .and_then(|trx| trx.get_read_version())
//        .map(|v| {
//            eprintln!("read version: {:?}", v);
//        });
//
//    fut.wait().expect("failed to run");
//}
//
//#[test]
//fn test_set_read_version() {
//    const KEY: &[u8] = b"test-versionstamp";
//    common::setup_static();
//
//    let fut = Cluster::new(foundationdb::default_config_path())
//        .and_then(|cluster| cluster.create_database())
//        .and_then(|db| result(db.create_trx()))
//        .and_then(|trx| {
//            trx.set_read_version(0);
//            trx.get(KEY, false)
//        })
//        .map(|_v| {
//            panic!("should fail with past_version");
//        })
//        .or_else(|e| {
//            eprintln!("failed as expeced: {:?}", e);
//            Ok::<(), ()>(())
//        });
//
//    fut.wait().expect("failed to run");
//}
