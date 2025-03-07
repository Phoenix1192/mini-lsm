#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{
    collections::HashSet,
    ops::Bound,
    sync::{atomic::AtomicBool, Arc},
};

use crate::{
    iterators::two_merge_iterator::TwoMergeIterator, lsm_storage::WriteBatchRecord,
    mem_table::map_bound_bytes,
};
use crate::{
    iterators::StorageIterator,
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::LsmStorageInner,
};
use anyhow::{bail, Result};
use bytes::Bytes;
use crossbeam_skiplist::{map::Entry, SkipMap};
use ouroboros::self_referencing;
use parking_lot::Mutex;
use std::ops::Bound::Excluded;
use std::sync::atomic::Ordering;

use super::CommittedTxnData;

// use super::CommittedTxnData;

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn panic_if_commited(&self) {
        let committed = self.committed.load(Ordering::SeqCst);
        if committed {
            panic!("The transaction is already comitted");
        }
    }
    pub fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        self.panic_if_commited();
        self.key_hashes
            .as_ref()
            .unwrap()
            .lock()
            .0
            .insert(farmhash::hash32(_key));
        if let Some(val) = self.local_storage.get(_key) {
            if val.value().is_empty() {
                Ok(None)
            } else {
                Ok(Some(val.value().clone()))
            }
        } else {
            LsmStorageInner::get_with_ts(&self.inner, _key, self.read_ts)
        }
    }

    pub fn scan(
        self: &Arc<Self>,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<TxnIterator> {
        self.panic_if_commited();
        let mut local_iter = TxnLocalIteratorBuilder {
            map: self.local_storage.clone(),
            iter_builder: |map| map.range((map_bound_bytes(_lower), map_bound_bytes(_upper))),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        let entry = local_iter.with_iter_mut(|iter| TxnLocalIterator::entry_to_item(iter.next()));
        local_iter.with_mut(|x| *x.item = entry);
        let mut iter = TxnIterator::create(
            self.clone(),
            TwoMergeIterator::create(
                local_iter,
                LsmStorageInner::scan_with_ts(&self.inner, _lower, _upper, self.read_ts)?,
            )?,
        )?;
        {
            let read_key_hash = &mut self.key_hashes.as_ref().unwrap().lock().0;
            // println!("Scan add: {:?}",Bytes::copy_from_slice(iter.key()));
            while iter.is_valid() {
                read_key_hash.insert(farmhash::hash32(iter.key()));
                iter.next()?;
            }
        }

        //New one is built
        let mut local_iter = TxnLocalIteratorBuilder {
            map: self.local_storage.clone(),
            iter_builder: |map| map.range((map_bound_bytes(_lower), map_bound_bytes(_upper))),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        let entry = local_iter.with_iter_mut(|iter| TxnLocalIterator::entry_to_item(iter.next()));
        local_iter.with_mut(|x| *x.item = entry);
        TxnIterator::create(
            self.clone(),
            TwoMergeIterator::create(
                local_iter,
                LsmStorageInner::scan_with_ts(&self.inner, _lower, _upper, self.read_ts)?,
            )?,
        )
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.panic_if_commited();
        self.key_hashes
            .as_ref()
            .unwrap()
            .lock()
            .1
            .insert(farmhash::hash32(key));
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    pub fn delete(&self, key: &[u8]) {
        self.panic_if_commited();
        self.key_hashes
            .as_ref()
            .unwrap()
            .lock()
            .1
            .insert(farmhash::hash32(key));
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::new());
    }

    pub fn commit(&self) -> Result<()> {
        let mut write_batch_records = Vec::new();
        for entry in self.local_storage.iter() {
            write_batch_records.push(WriteBatchRecord::Put(
                entry.key().to_vec(),
                entry.value().to_vec(),
            ));
        }

        let commit_guard = self.inner.mvcc().commit_lock.lock();
        let commit_ts_expected = {
            let temp_lock = self.inner.mvcc().write_lock.lock();
            let commit_ts_expected = self.inner.mvcc().latest_commit_ts() + 1;
            self.inner.mvcc().update_commit_ts(commit_ts_expected);
            commit_ts_expected
        };
        let mut commited_transactions = self.inner.mvcc().committed_txns.lock();

        // Remove Old transactions
        let watermark = self.inner.mvcc().watermark();
        while let Some(entry) = commited_transactions.first_entry() {
            if *entry.key() < watermark {
                entry.remove();
            } else {
                break;
            }
        }

        let write_entry_exists = { self.key_hashes.as_ref().unwrap().lock().1.len() > 0 };
        if write_entry_exists {
            // Get transactions within the range
            let check_transactions_iterator =
                commited_transactions.range((Excluded(self.read_ts), Excluded(commit_ts_expected)));
            // println!("Transactions were checked");
            for check_transaction in check_transactions_iterator {
                let key_hashes = { &self.key_hashes.as_ref().unwrap().lock().0 };
                for key_hash in key_hashes {
                    if check_transaction.1.key_hashes.contains(&key_hash) {
                        bail!("Transaction is not serializeable");
                    }
                }
            }
        }

        let commit_ts = self.inner.write_batch_inner(&write_batch_records[..])?;

        commited_transactions.insert(
            commit_ts,
            CommittedTxnData {
                key_hashes: self.key_hashes.as_ref().unwrap().lock().1.clone(),
                read_ts: self.read_ts,
                commit_ts: commit_ts,
            },
        );
        self.committed.store(true, Ordering::SeqCst);

        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.inner
            .mvcc
            .as_ref()
            .unwrap()
            .ts
            .lock()
            .1
            .remove_reader(self.read_ts);
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl TxnLocalIterator {
    fn entry_to_item(entry_data: Option<Entry<'_, Bytes, Bytes>>) -> (Bytes, Bytes) {
        entry_data
            .map(|x| (x.key().clone(), x.value().clone()))
            .unwrap_or_else(|| (Bytes::new(), Bytes::new()))
    }
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        &self.borrow_item().1[..]
    }

    fn key(&self) -> &[u8] {
        &self.borrow_item().0[..]
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let entry = self.with_iter_mut(|iter| TxnLocalIterator::entry_to_item(iter.next()));
        self.with_mut(|x| *x.item = entry);
        Ok(())
    }
}

pub struct TxnIterator {
    txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        let mut temp_self = Self { txn, iter };
        temp_self.move_to_non_delete()?;
        Ok(temp_self)
    }

    pub fn move_to_non_delete(&mut self) -> Result<()> {
        // Pass through empty (deleted) values
        while self.iter.is_valid() && self.iter.value().is_empty() {
            self.iter.next()?;
        }
        Ok(())
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a> = &'a [u8] where Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        self.move_to_non_delete()?;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
