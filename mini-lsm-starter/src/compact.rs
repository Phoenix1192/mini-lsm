#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;

use crate::key::{Key, KeySlice, TS_MAX};
use crate::manifest::ManifestRecord;
use crate::table::{SsTableBuilder, SsTableIterator};
use anyhow::Result;
use serde::{Deserialize, Serialize};

use std::sync::Arc;
use std::time::Duration;

pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::merge_iterator::MergeIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::SsTable;

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn finalize_builder(
        &self,
        mut builder: SsTableBuilder,
        result_sst: &mut Vec<Arc<SsTable>>,
    ) -> Result<SsTableBuilder> {
        let id = self.next_sst_id();
        result_sst.push(Arc::new(builder.build(
            id,
            Some(self.block_cache.clone()),
            self.path_of_sst(id),
        )?));
        builder = SsTableBuilder::new(self.options.block_size);
        Ok(builder)
    }

    fn merger<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>>(
        &self,
        mut merge_iter: I,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut result_sst = Vec::new();
        let mut builder = SsTableBuilder::new(self.options.block_size);
        let mut prev_key: Option<Vec<u8>> = None;
        let watermark = self.mvcc.as_ref().unwrap().watermark();
        let mut prev_ts = 0;

        while merge_iter.is_valid() {
            let key = merge_iter.key();
            let current_key = Some(key.key_ref());

            let is_new_key = current_key != prev_key.as_deref();
            if is_new_key {
                prev_ts = TS_MAX;
            }
            if is_new_key && builder.estimated_size() >= self.options.target_sst_size {
                builder = self.finalize_builder(builder, &mut result_sst)?;
            }

            let is_visible =
                (key.ts() > watermark) || (prev_ts > watermark && !merge_iter.value().is_empty());

            if is_visible {
                builder.add(merge_iter.key(), merge_iter.value());
            }

            prev_key = current_key.map(|key| key.to_vec());
            prev_ts = merge_iter.key().ts();
            merge_iter.next()?;
        }

        // Builder is non empty
        if prev_key.is_some() {
            self.finalize_builder(builder, &mut result_sst)?;
        }

        Ok(result_sst)
    }

    fn merge_l0_l1(
        &self,
        l0_sstables: &[usize],
        l1_sstables: &[usize],
    ) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = { self.state.read().clone() };

        let sst_iters_l0 = l0_sstables
            .iter()
            .map(|sst_id| {
                SsTableIterator::create_and_seek_to_first(snapshot.sstables[sst_id].clone())
                    .map(Box::new)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let ss_tables_l1 = self.collect_tables(l1_sstables, &snapshot);

        let merge_iter = TwoMergeIterator::create(
            MergeIterator::create(sst_iters_l0),
            SstConcatIterator::create_and_seek_to_first(ss_tables_l1)?,
        )?;
        self.merger(merge_iter)
    }

    fn merge_levels(
        &self,
        table_upper: &[usize],
        table_lower: &[usize],
    ) -> Result<Vec<Arc<SsTable>>> {
        // Level 0 is highest
        let snapshot = { self.state.read().clone() };

        let ss_tables_upper = self.collect_tables(table_upper, &snapshot);
        let ss_tables_lower = self.collect_tables(table_lower, &snapshot);

        let merge_iter = TwoMergeIterator::create(
            SstConcatIterator::create_and_seek_to_first(ss_tables_upper)?,
            SstConcatIterator::create_and_seek_to_first(ss_tables_lower)?,
        )?;
        self.merger(merge_iter)
    }

    fn collect_tables(
        &self,
        ss_table_ids: &[usize],
        snapshot: &Arc<LsmStorageState>,
    ) -> Vec<Arc<SsTable>> {
        ss_table_ids
            .iter()
            .map(|sst_id| snapshot.sstables[sst_id].clone())
            .collect()
    }

    fn concat_iterator(
        &self,
        ss_table_ids: &[usize],
        snapshot: &Arc<LsmStorageState>,
    ) -> Result<Box<SstConcatIterator>> {
        let ss_tables = self.collect_tables(ss_table_ids, snapshot);
        Ok(Box::new(SstConcatIterator::create_and_seek_to_first(
            ss_tables,
        )?))
    }

    fn l0_l1_ids<'a>(&self, task: &'a CompactionTask) -> Option<(&'a Vec<usize>, &'a Vec<usize>)> {
        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => Some((l0_sstables, l1_sstables)),
            CompactionTask::Simple(simple_task) if simple_task.upper_level.is_none() => Some((
                &simple_task.upper_level_sst_ids,
                &simple_task.lower_level_sst_ids,
            )),
            CompactionTask::Leveled(leveled_task) if leveled_task.upper_level.is_none() => Some((
                &leveled_task.upper_level_sst_ids,
                &leveled_task.lower_level_sst_ids,
            )),
            _ => None,
        }
    }

    fn level_ids<'a>(&self, task: &'a CompactionTask) -> Option<(&'a Vec<usize>, &'a Vec<usize>)> {
        match task {
            CompactionTask::Simple(simple_task) if !simple_task.upper_level.is_none() => Some((
                &simple_task.upper_level_sst_ids,
                &simple_task.lower_level_sst_ids,
            )),
            CompactionTask::Leveled(leveled_task) if !leveled_task.upper_level.is_none() => Some((
                &leveled_task.upper_level_sst_ids,
                &leveled_task.lower_level_sst_ids,
            )),
            _ => None,
        }
    }
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        if let Some((l0_sstables, l1_sstables)) = self.l0_l1_ids(task) {
            self.merge_l0_l1(l0_sstables, l1_sstables)
        } else if let Some((upper_level_sst_ids, lower_level_sst_ids)) = self.level_ids(task) {
            self.merge_levels(upper_level_sst_ids, lower_level_sst_ids)
        } else if let CompactionTask::Tiered(tiered_task) = task {
            let snapshot = { self.state.read().clone() };

            let iterator_ssts = tiered_task
                .tiers
                .iter()
                .map(|(_, tier)| self.concat_iterator(tier, &snapshot))
                .collect::<Result<Vec<_>, _>>()?;

            self.merger(MergeIterator::create(iterator_ssts))
        } else {
            panic!("The parsing logic for the tasks has failed")
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let (l0_sstables, l1_sstables) = {
            let guard = self.state.read();
            (guard.l0_sstables.clone(), guard.levels[0].1.clone())
        };

        // Compacted SSTs
        let new_ssts = self.compact(&CompactionTask::ForceFullCompaction {
            l0_sstables: (l0_sstables.clone()),
            l1_sstables: (l1_sstables.clone()),
        })?;

        {
            let _lock = self.state_lock.lock();
            let mut new_state = self.state.read().as_ref().clone();

            // Remove the tables that were compacted
            new_state.l0_sstables.retain(|x| !l0_sstables.contains(x));

            // Add the new SSTables to L1
            let new_sst_ids: Vec<_> = new_ssts.iter().map(|x| x.sst_id()).collect();
            new_state.levels[0].1 = new_sst_ids.clone();

            // Update the Sstables maps
            for sst_ids in l0_sstables.iter().chain(l1_sstables.iter()) {
                new_state.sstables.remove(sst_ids);
            }
            for (sst_id, sst) in new_sst_ids.iter().zip(new_ssts.iter()) {
                new_state.sstables.insert(*sst_id, sst.clone());
            }

            *self.state.write() = Arc::new(new_state);
        }
        Ok(())
    }

    pub fn del_tables(
        sst_ids: Vec<usize>,
        snapshot: &mut LsmStorageState,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut deleted_tables = Vec::new();
        for sst_id in sst_ids.iter() {
            let res = snapshot.sstables.remove(sst_id);
            if res.is_none() {
                panic!("!Deletion Id not found in the table")
            }
            deleted_tables.push(res.unwrap());
        }
        Ok(deleted_tables)
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot_copy = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        let task = self
            .compaction_controller
            .generate_compaction_task(&snapshot_copy);
        if task.is_none() {
            return Ok(());
        }

        let output = self.compact(task.as_ref().unwrap())?;
        let output_ids_vec: Vec<usize> = output.iter().map(|x| x.as_ref().sst_id()).collect();
        let output_ids: &[usize] = &output_ids_vec;
        let ssts_to_remove = {
            let state_lock = self.state_lock.lock();
            let mut snapshot_new = self.state.read().as_ref().clone();
            for i in 0..output.len() {
                snapshot_new
                    .sstables
                    .insert(output_ids[i], output[i].clone());
            }

            let (mut snapshot_new, del) = self.compaction_controller.apply_compaction_result(
                &snapshot_new,
                task.as_ref().unwrap(),
                output_ids,
                false,
            );
            let ssts_to_remove = LsmStorageInner::del_tables(del, &mut snapshot_new)?;

            let mut state = self.state.write();
            *state = Arc::new(snapshot_new);
            drop(state);

            self.manifest.as_ref().unwrap().add_record(
                &state_lock,
                ManifestRecord::Compaction(task.unwrap(), output_ids_vec.clone()),
            )?;

            ssts_to_remove
        };
        for table_id in ssts_to_remove {
            std::fs::remove_file(self.path_of_sst(table_id.sst_id()))?;
        }
        self.sync_dir()?;

        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let res = {
            let guard = self.state.read();
            guard.imm_memtables.len() >= self.options.num_memtable_limit
        };
        if res {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
