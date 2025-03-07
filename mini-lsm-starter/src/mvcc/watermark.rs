#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use core::panic;
use std::collections::BTreeMap;

pub struct Watermark {
    readers: BTreeMap<u64, usize>,
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: BTreeMap::new(),
        }
    }

    pub fn add_reader(&mut self, ts: u64) {
        let previous_value = self.readers.get(&ts).unwrap_or(&0);
        self.readers.insert(ts, *previous_value + 1);
    }

    pub fn remove_reader(&mut self, ts: u64) {
        if let Some(previous_value) = self.readers.get(&ts) {
            if *previous_value == 1 {
                self.readers.remove(&ts);
            } else {
                self.readers.insert(ts, *previous_value - 1);
            }
        } else {
            panic!("Attempted to remove a reader that is not present in the watermark")
        }
    }

    pub fn watermark(&self) -> Option<u64> {
        self.readers.first_key_value().map(|(k, _)| *k)
    }

    pub fn num_retained_snapshots(&self) -> usize {
        self.readers.len()
    }
}

impl Default for Watermark {
    fn default() -> Self {
        Self::new()
    }
}
