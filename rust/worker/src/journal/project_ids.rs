//! Durable project-ID allocation for an exclusively owned quota filesystem.
use super::{files::Directory, JournalError, JournalLimits};
use crate::control::canonical_uuid;
use std::path::Path;

const COUNTER: &str = ".project-ids";
const EXHAUSTED: u64 = i32::MAX as u64 + 1;

pub struct ProjectIds {
    directory: Directory,
}

impl ProjectIds {
    /// Initialize an empty private control directory once. The caller must own
    /// the entire filesystem namespace; a different worker cannot share it.
    pub fn initialize(root: &Path, worker: &str) -> Result<Self, JournalError> {
        if !canonical_uuid(worker) {
            return Err(JournalError::Invalid);
        }
        let mut directory = Directory::open(root, JournalLimits::default())?;
        if !directory.inventory()?.is_empty()
            || directory.read(".identity")?.is_some()
            || directory.read(".session")?.is_some()
            || directory.read(COUNTER)?.is_some()
        {
            return Err(JournalError::Conflict);
        }
        directory.write(".identity", worker.as_bytes())?;
        directory.write(COUNTER, &1u64.to_be_bytes())?;
        Ok(Self { directory })
    }

    pub fn open(root: &Path, worker: &str) -> Result<Self, JournalError> {
        if !canonical_uuid(worker) {
            return Err(JournalError::Invalid);
        }
        let directory = Directory::open(root, JournalLimits::default())?;
        match directory.read(".identity")? {
            Some(identity) if identity == worker.as_bytes() => {}
            Some(_) => return Err(JournalError::Identity),
            None => return Err(JournalError::Corrupt),
        }
        if !directory.inventory()?.is_empty() || directory.read(".session")?.is_some() {
            return Err(JournalError::Corrupt);
        }
        let store = Self { directory };
        store.next()?;
        Ok(store)
    }

    pub fn reserve(&mut self) -> Result<u32, JournalError> {
        let next = self.next()?;
        if next == EXHAUSTED {
            return Err(JournalError::Limit);
        }
        // INVARIANT: publish the next counter durably before exposing this ID.
        // A crash can skip an ID, but an ID returned for filesystem setup cannot
        // be reused. Uncertain writes poison the shared atomic record writer.
        self.directory.write(COUNTER, &(next + 1).to_be_bytes())?;
        Ok(next as u32)
    }

    pub fn verify(&self) -> Result<(), JournalError> {
        self.next().map(|_| ())
    }

    fn next(&self) -> Result<u64, JournalError> {
        let bytes = self.directory.read(COUNTER)?.ok_or(JournalError::Corrupt)?;
        let next = u64::from_be_bytes(bytes.try_into().map_err(|_| JournalError::Corrupt)?);
        if !(1..=EXHAUSTED).contains(&next) {
            return Err(JournalError::Corrupt);
        }
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::{files::CommitStep, new_uuid};
    use std::{fs, os::unix::fs::DirBuilderExt};

    const WORKER: &str = "11111111-1111-4111-8111-111111111111";

    #[test]
    fn failed_reservations_poison_the_writer_and_reopen_without_reusing_returned_ids() {
        for step in [
            CommitStep::Created,
            CommitStep::Written,
            CommitStep::Synced,
            CommitStep::Renamed,
            CommitStep::Committed,
        ] {
            let root = std::env::temp_dir()
                .join(format!("dispatch-project-id-fault-{}", new_uuid().unwrap()));
            fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
            let root = root.canonicalize().unwrap();
            let mut ids = ProjectIds::initialize(&root, WORKER).unwrap();
            assert_eq!(ids.reserve().unwrap(), 1);
            ids.directory.fault = Some(step);
            assert!(ids.reserve().is_err());
            assert!(matches!(ids.reserve(), Err(JournalError::Poisoned)));
            drop(ids);
            let mut reopened = ProjectIds::open(&root, WORKER).unwrap();
            let expected = if matches!(step, CommitStep::Renamed | CommitStep::Committed) {
                3
            } else {
                2
            };
            assert_eq!(reopened.reserve().unwrap(), expected);
            drop(reopened);
            fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn exhausted_namespace_never_wraps_to_zero_or_reuses_an_id() {
        let root =
            std::env::temp_dir().join(format!("dispatch-project-id-limit-{}", new_uuid().unwrap()));
        fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
        let root = root.canonicalize().unwrap();
        let mut ids = ProjectIds::initialize(&root, WORKER).unwrap();
        ids.directory
            .write(COUNTER, &(EXHAUSTED - 1).to_be_bytes())
            .unwrap();
        assert_eq!(ids.reserve().unwrap(), i32::MAX as u32);
        assert!(matches!(ids.reserve(), Err(JournalError::Limit)));
        drop(ids);
        let mut reopened = ProjectIds::open(&root, WORKER).unwrap();
        assert!(matches!(reopened.reserve(), Err(JournalError::Limit)));
        drop(reopened);
        fs::remove_dir_all(root).unwrap();
    }
}
