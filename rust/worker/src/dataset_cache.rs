//! Deterministic pin-aware admission and LRU eviction policy for local inputs.

use std::{collections::HashMap, fmt};

#[derive(Debug, PartialEq, Eq)]
pub enum CacheError {
    Configuration,
    InvalidEntry,
    Missing,
    Pinned,
    Capacity,
}

impl fmt::Display for CacheError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("dataset cache policy rejected operation")
    }
}
impl std::error::Error for CacheError {}

struct Entry {
    size_bytes: u64,
    pins: u64,
    last_used: u64,
}

pub struct CacheIndex {
    high_bytes: u64,
    low_bytes: u64,
    used_bytes: u64,
    clock: u64,
    entries: HashMap<String, Entry>,
}

impl CacheIndex {
    pub fn new(high_bytes: u64, low_bytes: u64) -> Result<Self, CacheError> {
        if low_bytes == 0 || high_bytes <= low_bytes {
            return Err(CacheError::Configuration);
        }
        Ok(Self {
            high_bytes,
            low_bytes,
            used_bytes: 0,
            clock: 0,
            entries: HashMap::new(),
        })
    }

    pub fn used_bytes(&self) -> u64 {
        self.used_bytes
    }

    pub fn insert(&mut self, key: &str, size_bytes: u64) -> Result<(), CacheError> {
        if key.is_empty() || key.len() > 128 || size_bytes == 0 || self.entries.contains_key(key) {
            return Err(CacheError::InvalidEntry);
        }
        let used = self
            .used_bytes
            .checked_add(size_bytes)
            .filter(|used| *used <= self.high_bytes)
            .ok_or(CacheError::Capacity)?;
        self.clock = self.clock.checked_add(1).ok_or(CacheError::Capacity)?;
        self.entries.insert(
            key.to_owned(),
            Entry {
                size_bytes,
                pins: 0,
                last_used: self.clock,
            },
        );
        self.used_bytes = used;
        Ok(())
    }

    pub fn pin(&mut self, key: &str) -> Result<(), CacheError> {
        let entry = self.entries.get_mut(key).ok_or(CacheError::Missing)?;
        let pins = entry.pins.checked_add(1).ok_or(CacheError::Capacity)?;
        let clock = self.clock.checked_add(1).ok_or(CacheError::Capacity)?;
        entry.pins = pins;
        entry.last_used = clock;
        self.clock = clock;
        Ok(())
    }

    pub fn unpin(&mut self, key: &str) -> Result<(), CacheError> {
        let entry = self.entries.get_mut(key).ok_or(CacheError::Missing)?;
        if entry.pins == 0 {
            return Err(CacheError::InvalidEntry);
        }
        entry.pins -= 1;
        Ok(())
    }

    pub fn plan_eviction(&self, incoming_bytes: u64) -> Result<Vec<String>, CacheError> {
        if incoming_bytes == 0 || incoming_bytes > self.high_bytes {
            return Err(CacheError::Capacity);
        }
        let mut projected = self
            .used_bytes
            .checked_add(incoming_bytes)
            .ok_or(CacheError::Capacity)?;
        if projected <= self.high_bytes {
            return Ok(Vec::new());
        }
        let mut candidates: Vec<_> = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.pins == 0)
            .collect();
        candidates.sort_by(|(left_key, left), (right_key, right)| {
            (left.last_used, left_key).cmp(&(right.last_used, right_key))
        });
        let mut victims = Vec::new();
        // Prefer the low watermark to avoid repeated small evictions. If pins
        // prevent it, enough unpinned space to fit below high is still safe.
        for (key, entry) in candidates {
            if projected <= self.low_bytes {
                break;
            }
            projected -= entry.size_bytes;
            victims.push(key.clone());
        }
        if projected > self.high_bytes {
            Err(CacheError::Capacity)
        } else {
            Ok(victims)
        }
    }

    pub fn remove(&mut self, key: &str) -> Result<(), CacheError> {
        let entry = self.entries.get(key).ok_or(CacheError::Missing)?;
        // INVARIANT: a running attempt's pin prevents the cache entry from
        // being selected or removed, even when capacity is exhausted.
        if entry.pins != 0 {
            return Err(CacheError::Pinned);
        }
        self.used_bytes -= entry.size_bytes;
        self.entries.remove(key);
        Ok(())
    }
}
