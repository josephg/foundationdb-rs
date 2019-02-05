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

use foundationdb::future::Wait;

use foundationdb::*;
use futures::executor::*;

mod common;

fn setup() -> Database {
    common::setup_static();
    let cluster = Cluster::new(foundationdb::default_config_path()).wait().unwrap();
    let db = cluster.create_database().wait().unwrap();
    db
}

#[test]
fn test_set_get() -> Result<(), Error> {
    let db = setup();

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
    let db = setup();

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
    let db = setup();
    let key = b"test-conflict";

    block_on(async || -> Result<(), Error> {
        let trx1 = db.create_trx()?;
        await!(trx1.get(key, false))?;

        // Commit concurrent transaction to create a conflict.
        await!(db.transact(async move |trx2| {
            trx2.set(key, common::random_str(10).as_bytes());
            Ok(())
        }))?;

        trx1.set(key, common::random_str(10).as_bytes());
        let r = await!(trx1.commit());

//        println!("{:?}", r);
        // 1020: "Transaction not committed due to conflict with another transaction"
        assert_eq!(r.err().expect("Transaction should have conflicted").code().unwrap().get(), 1020);
        Ok(())
    }())
}


#[test]
fn test_set_conflict_snapshot() -> Result<(), Error> {
    let db = setup();
    let key = b"test-conflict-snapshot";

    block_on(async || -> Result<(), Error> {
        let trx1 = db.create_trx()?;
        await!(trx1.get(key, true))?;

        // Commit concurrent transaction to create a conflict.
        await!(db.transact(async move |trx2| {
            trx2.set(key, common::random_str(10).as_bytes());
            Ok(())
        }))?;

        trx1.set(key, common::random_str(10).as_bytes());
        await!(trx1.commit()) // Should succeed this time.
    }())
}

// Makes the key dirty. It will abort transactions which performs non-snapshot read on the `key`.
async fn make_dirty<'a>(db: &'a Database, key: &'a [u8]) {
    await!(db.transact(async move |trx| {
        trx.set(key, b"");
        Ok(())
    })).unwrap();
}

#[test]
fn test_transact() -> Result<(), Error> {
    use std::sync::{atomic::*, Arc};

    const KEY: &[u8] = b"test-transact";
    const RETRY_COUNT: usize = 5;

    let try_count = Arc::new(AtomicUsize::new(0));
    let try_count_ref = &try_count; // Needed because we have to `async move`.

    let db = setup();
    let db_ref = &db;

    block_on(async || -> Result<(), Error> {
        let result = await!(db.transact(async move |trx| {
            // increment try counter
            try_count_ref.fetch_add(1, Ordering::SeqCst);

            trx.set_option(options::TransactionOption::RetryLimit(RETRY_COUNT as u32))
                .expect("failed to set retry limit");

            // update conflict range
            await!(trx.get(KEY, false))?;

            // make current transaction invalid by making conflict
            await!(make_dirty(db_ref, KEY));

            trx.set(KEY, common::random_str(10).as_bytes());
            // `Database::transact` will handle commit by itself, so returns without commit
            Ok(())
        }));

        result.err().expect("should not be able to commit");
        Ok(())
    }())
}

#[test]
fn test_versionstamp() -> Result<(), Error> {
    const KEY: &[u8] = b"test-versionstamp";
    let db = setup();

    let vs = block_on(async || -> Result<_, Error> {
        // Note whats happening here - the transaction must be committed before we can pull the versionstamp future out and wait on it.
        let vs_fut = await!(db.transact(async move |trx| {
            trx.set(KEY, common::random_str(10).as_bytes());
            Ok(trx.get_versionstamp())
        }))?;

        await!(vs_fut)
    }())?;

    println!("versionstamp: {:?}", *vs);
    Ok(())
}

#[test]
fn test_read_version() -> Result<(), Error> {
    let db = setup();

    block_on(async || -> Result<_, Error> {
        let trx = db.create_trx()?;
        let read_version = await!(trx.get_read_version())?;

        println!("read version: {:?}", read_version);
        Ok(())
    }())
}

#[test]
fn test_set_read_version() {
    const KEY: &[u8] = b"test-versionstamp";
    let db = setup();

    let result = block_on(async || -> Result<_, Error> {
        let trx = db.create_trx()?;
        trx.set_read_version(1000);
        await!(trx.get(KEY, false))
    }());

    assert_eq!(
        result.err().expect("Should fail with transaction_too_old").code().unwrap().get(),
        1007);
}
