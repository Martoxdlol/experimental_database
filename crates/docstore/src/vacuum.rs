//! D7: Vacuum — WAL-driven vacuum and rollback vacuum.
//!
//! Two mechanisms:
//! 1. **WAL-driven vacuum**: incrementally removes old document versions
//!    that are no longer visible to any reader.
//! 2. **Rollback vacuum**: removes entries from commits that were never
//!    replicated (`ts > visible_ts`).

use crate::key_encoding::make_primary_key;
use crate::primary_index::{CellFlags, PrimaryIndex};
use crate::secondary_index::SecondaryIndex;
use exdb_core::types::{CollectionId, DocId, IndexId, Ts};
use std::collections::HashMap;
use std::sync::Arc;

// ─── WAL-Driven Vacuum ───

/// A candidate for vacuum: an old version that has been superseded.
pub struct VacuumCandidate {
    pub collection_id: CollectionId,
    pub doc_id: DocId,
    /// Timestamp of the old version being superseded.
    pub old_ts: Ts,
    /// Timestamp of the new version that supersedes the old one.
    pub superseding_ts: Ts,
    /// Secondary index keys that belonged to the old version.
    /// Each entry is `(index_id, encoded_key)`.
    pub old_index_keys: Vec<(IndexId, Vec<u8>)>,
}

/// Manages the pending vacuum queue and executes vacuum passes.
pub struct VacuumCoordinator {
    pending: Vec<VacuumCandidate>,
}

impl VacuumCoordinator {
    /// Create a new empty coordinator.
    pub fn new() -> Self {
        Self {
            pending: Vec::new(),
        }
    }

    /// Push a new vacuum candidate (called from commit path).
    pub fn push_candidate(&mut self, candidate: VacuumCandidate) {
        self.pending.push(candidate);
    }

    /// Drain all candidates eligible for vacuum.
    ///
    /// A candidate is eligible if `candidate.superseding_ts <= vacuum_safe_ts`.
    pub fn drain_eligible(&mut self, vacuum_safe_ts: Ts) -> Vec<VacuumCandidate> {
        let mut eligible = Vec::new();
        let mut remaining = Vec::new();
        for c in self.pending.drain(..) {
            if c.superseding_ts <= vacuum_safe_ts {
                eligible.push(c);
            } else {
                remaining.push(c);
            }
        }
        self.pending = remaining;
        eligible
    }

    /// Execute vacuum: remove old entries from primary and secondary indexes.
    ///
    /// Returns the number of entries removed.
    pub fn execute(
        &self,
        candidates: &[VacuumCandidate],
        primary_indexes: &HashMap<CollectionId, Arc<PrimaryIndex>>,
        secondary_indexes: &HashMap<IndexId, Arc<SecondaryIndex>>,
    ) -> std::io::Result<u64> {
        let mut removed = 0u64;

        for candidate in candidates {
            // Remove primary entry
            if let Some(primary) = primary_indexes.get(&candidate.collection_id) {
                let key = make_primary_key(&candidate.doc_id, candidate.old_ts);

                // Check if entry was external (heap-stored body)
                if let Some(value) = primary.btree().get(&key)? {
                    let flags = CellFlags::from_byte(value[0]);
                    if flags.external && !flags.tombstone {
                        let href_bytes: [u8; 6] = value[5..11].try_into().unwrap();
                        let href = exdb_storage::heap::HeapRef::from_bytes(&href_bytes);
                        primary.engine().heap_free(href)?;
                    }
                }

                if primary.btree().delete(&key)? {
                    removed += 1;
                }
            }

            // Remove secondary entries
            for (index_id, encoded_key) in &candidate.old_index_keys {
                if let Some(secondary) = secondary_indexes.get(index_id)
                    && secondary.remove_entry(encoded_key)?
                {
                    removed += 1;
                }
            }
        }

        Ok(removed)
    }

    /// Rebuild the pending queue from WAL replay: push a commit candidate.
    pub fn replay_commit(&mut self, candidate: VacuumCandidate) {
        self.pending.push(candidate);
    }

    /// Rebuild the pending queue from WAL replay: remove a vacuumed candidate.
    pub fn replay_vacuum(&mut self, collection_id: CollectionId, doc_id: &DocId, old_ts: Ts) {
        self.pending.retain(|c| {
            !(c.collection_id == collection_id && c.doc_id == *doc_id && c.old_ts == old_ts)
        });
    }

    /// Number of pending candidates.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
}

impl Default for VacuumCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Rollback Vacuum ───

/// Information extracted from a TxCommit WAL record for rollback.
pub struct WalCommitInfo {
    pub commit_ts: Ts,
    pub mutations: Vec<(CollectionId, DocId)>,
    /// `(index_id, new_key to remove)`.
    pub index_deltas: Vec<(IndexId, Option<Vec<u8>>)>,
}

/// Removes entries written by commits that were never replicated.
pub struct RollbackVacuum;

impl RollbackVacuum {
    /// Live rollback: undo a single failed commit using its in-memory write set.
    pub fn rollback_commit(
        commit_ts: Ts,
        mutations: &[(CollectionId, DocId)],
        index_deltas: &[(IndexId, Option<Vec<u8>>)],
        primary_indexes: &HashMap<CollectionId, Arc<PrimaryIndex>>,
        secondary_indexes: &HashMap<IndexId, Arc<SecondaryIndex>>,
    ) -> std::io::Result<()> {
        for (collection_id, doc_id) in mutations {
            if let Some(primary) = primary_indexes.get(collection_id) {
                let key = make_primary_key(doc_id, commit_ts);
                primary.btree().delete(&key)?;
            }
        }
        for (index_id, new_key) in index_deltas {
            if let Some(key) = new_key
                && let Some(secondary) = secondary_indexes.get(index_id)
            {
                secondary.remove_entry(key)?;
            }
        }
        Ok(())
    }

    /// Startup cleanup: undo all commits with `ts > visible_ts` using WAL.
    ///
    /// Returns the number of rolled-back commits.
    pub fn rollback_from_wal(
        _visible_ts: Ts,
        wal_commits: &[WalCommitInfo],
        primary_indexes: &HashMap<CollectionId, Arc<PrimaryIndex>>,
        secondary_indexes: &HashMap<IndexId, Arc<SecondaryIndex>>,
    ) -> std::io::Result<u64> {
        let mut count = 0u64;
        for commit in wal_commits {
            Self::rollback_commit(
                commit.commit_ts,
                &commit.mutations,
                &commit.index_deltas,
                primary_indexes,
                secondary_indexes,
            )?;
            count += 1;
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key_encoding::make_secondary_key;
    use exdb_core::types::Scalar;
    use exdb_storage::engine::{StorageConfig, StorageEngine};

    type Setup = (
        Arc<StorageEngine>,
        HashMap<CollectionId, Arc<PrimaryIndex>>,
        HashMap<IndexId, Arc<SecondaryIndex>>,
    );

    fn setup() -> Setup {
        let engine = Arc::new(StorageEngine::open_in_memory(StorageConfig::default()).unwrap());
        let primary_btree = engine.create_btree().unwrap();
        let primary = Arc::new(PrimaryIndex::new(primary_btree, engine.clone(), 4096));

        let sec_btree = engine.create_btree().unwrap();
        let secondary = Arc::new(SecondaryIndex::new(sec_btree, primary.clone()));

        let mut primaries = HashMap::new();
        primaries.insert(CollectionId(1), primary);
        let mut secondaries = HashMap::new();
        secondaries.insert(IndexId(1), secondary);

        (engine, primaries, secondaries)
    }

    fn doc(n: u8) -> DocId {
        let mut bytes = [0u8; 16];
        bytes[15] = n;
        DocId(bytes)
    }

    #[tokio::test]
    async fn push_and_drain() {
        let mut vc = VacuumCoordinator::new();
        vc.push_candidate(VacuumCandidate {
            collection_id: CollectionId(1),
            doc_id: doc(1),
            old_ts: 1,
            superseding_ts: 5,
            old_index_keys: vec![],
        });
        vc.push_candidate(VacuumCandidate {
            collection_id: CollectionId(1),
            doc_id: doc(2),
            old_ts: 2,
            superseding_ts: 10,
            old_index_keys: vec![],
        });
        vc.push_candidate(VacuumCandidate {
            collection_id: CollectionId(1),
            doc_id: doc(3),
            old_ts: 3,
            superseding_ts: 15,
            old_index_keys: vec![],
        });

        let eligible = vc.drain_eligible(12);
        assert_eq!(eligible.len(), 2);
        assert_eq!(vc.pending_count(), 1);
    }

    #[tokio::test]
    async fn execute_removes_primary() {
        let (_engine, primaries, secondaries) = setup();
        let primary = primaries.get(&CollectionId(1)).unwrap();
        let d = doc(1);
        primary.insert_version(&d, 5, Some(b"old")).unwrap();
        primary.insert_version(&d, 10, Some(b"new")).unwrap();

        let vc = VacuumCoordinator::new();
        let candidates = vec![VacuumCandidate {
            collection_id: CollectionId(1),
            doc_id: d,
            old_ts: 5,
            superseding_ts: 10,
            old_index_keys: vec![],
        }];
        let removed = vc.execute(&candidates, &primaries, &secondaries).unwrap();
        assert!(removed > 0);

        // Old version is gone, but new version still accessible
        assert!(primary.get_at_ts(&d, 5).unwrap().is_none());
        assert_eq!(primary.get_at_ts(&d, 10).unwrap().unwrap(), b"new");
    }

    #[tokio::test]
    async fn execute_removes_secondary() {
        let (_engine, primaries, secondaries) = setup();
        let primary = primaries.get(&CollectionId(1)).unwrap();
        let secondary = secondaries.get(&IndexId(1)).unwrap();
        let d = doc(1);

        primary.insert_version(&d, 5, Some(b"old")).unwrap();
        let sec_key = make_secondary_key(&[Scalar::String("x".into())], &d, 5);
        secondary.insert_entry(&sec_key).unwrap();

        primary.insert_version(&d, 10, Some(b"new")).unwrap();

        let vc = VacuumCoordinator::new();
        let candidates = vec![VacuumCandidate {
            collection_id: CollectionId(1),
            doc_id: d,
            old_ts: 5,
            superseding_ts: 10,
            old_index_keys: vec![(IndexId(1), sec_key)],
        }];
        let removed = vc.execute(&candidates, &primaries, &secondaries).unwrap();
        assert!(removed >= 2); // primary + secondary
    }

    #[tokio::test]
    async fn rollback_single_commit() {
        let (_engine, primaries, secondaries) = setup();
        let primary = primaries.get(&CollectionId(1)).unwrap();
        let d = doc(1);
        primary.insert_version(&d, 10, Some(b"body")).unwrap();

        RollbackVacuum::rollback_commit(
            10,
            &[(CollectionId(1), d)],
            &[],
            &primaries,
            &secondaries,
        )
        .unwrap();

        assert!(primary.get_at_ts(&d, 10).unwrap().is_none());
    }

    #[tokio::test]
    async fn rollback_preserves_old_version() {
        let (_engine, primaries, secondaries) = setup();
        let primary = primaries.get(&CollectionId(1)).unwrap();
        let d = doc(1);
        primary.insert_version(&d, 5, Some(b"v5")).unwrap();
        primary.insert_version(&d, 10, Some(b"v10")).unwrap();

        RollbackVacuum::rollback_commit(
            10,
            &[(CollectionId(1), d)],
            &[],
            &primaries,
            &secondaries,
        )
        .unwrap();

        assert_eq!(primary.get_at_ts(&d, 5).unwrap().unwrap(), b"v5");
        assert_eq!(primary.get_at_ts(&d, 10).unwrap().unwrap(), b"v5"); // falls back to v5
    }

    #[tokio::test]
    async fn rollback_with_index_deltas() {
        let (_engine, primaries, secondaries) = setup();
        let primary = primaries.get(&CollectionId(1)).unwrap();
        let secondary = secondaries.get(&IndexId(1)).unwrap();
        let d = doc(1);

        primary.insert_version(&d, 10, Some(b"body")).unwrap();
        let sec_key = make_secondary_key(&[Scalar::String("x".into())], &d, 10);
        secondary.insert_entry(&sec_key).unwrap();

        RollbackVacuum::rollback_commit(
            10,
            &[(CollectionId(1), d)],
            &[(IndexId(1), Some(sec_key.clone()))],
            &primaries,
            &secondaries,
        )
        .unwrap();

        // Secondary entry should be gone
        assert!(secondary.btree().get(&sec_key).unwrap().is_none());
    }

    #[tokio::test]
    async fn replay_rebuild() {
        let mut vc = VacuumCoordinator::new();
        vc.replay_commit(VacuumCandidate {
            collection_id: CollectionId(1),
            doc_id: doc(1),
            old_ts: 1,
            superseding_ts: 5,
            old_index_keys: vec![],
        });
        vc.replay_commit(VacuumCandidate {
            collection_id: CollectionId(1),
            doc_id: doc(2),
            old_ts: 2,
            superseding_ts: 10,
            old_index_keys: vec![],
        });
        assert_eq!(vc.pending_count(), 2);

        vc.replay_vacuum(CollectionId(1), &doc(1), 1);
        assert_eq!(vc.pending_count(), 1);
    }

    #[tokio::test]
    async fn idempotent_vacuum() {
        let (_engine, primaries, secondaries) = setup();
        let primary = primaries.get(&CollectionId(1)).unwrap();
        let d = doc(1);
        primary.insert_version(&d, 5, Some(b"old")).unwrap();
        primary.insert_version(&d, 10, Some(b"new")).unwrap();

        let vc = VacuumCoordinator::new();
        let candidates = vec![VacuumCandidate {
            collection_id: CollectionId(1),
            doc_id: d,
            old_ts: 5,
            superseding_ts: 10,
            old_index_keys: vec![],
        }];
        vc.execute(&candidates, &primaries, &secondaries).unwrap();
        // Second time: no-op (key already deleted)
        let removed = vc.execute(&candidates, &primaries, &secondaries).unwrap();
        assert_eq!(removed, 0);
    }

    #[tokio::test]
    async fn empty_candidates() {
        let (_engine, primaries, secondaries) = setup();
        let vc = VacuumCoordinator::new();
        let removed = vc.execute(&[], &primaries, &secondaries).unwrap();
        assert_eq!(removed, 0);
    }

    #[tokio::test]
    async fn drain_eligible_none() {
        let mut vc = VacuumCoordinator::new();
        vc.push_candidate(VacuumCandidate {
            collection_id: CollectionId(1),
            doc_id: doc(1),
            old_ts: 1,
            superseding_ts: 15,
            old_index_keys: vec![],
        });
        let eligible = vc.drain_eligible(10);
        assert!(eligible.is_empty());
        assert_eq!(vc.pending_count(), 1);
    }

    #[tokio::test]
    async fn drain_eligible_correctness() {
        let mut vc = VacuumCoordinator::new();
        vc.push_candidate(VacuumCandidate {
            collection_id: CollectionId(1),
            doc_id: doc(1),
            old_ts: 3,
            superseding_ts: 8,
            old_index_keys: vec![],
        });

        // vacuum_safe_ts=7: superseding_ts=8 > 7, NOT eligible
        let eligible = vc.drain_eligible(7);
        assert!(eligible.is_empty());

        // vacuum_safe_ts=8: superseding_ts=8 <= 8, eligible
        let eligible = vc.drain_eligible(8);
        assert_eq!(eligible.len(), 1);
    }

    #[tokio::test]
    async fn rollback_from_wal() {
        let (_engine, primaries, secondaries) = setup();
        let primary = primaries.get(&CollectionId(1)).unwrap();
        let d1 = doc(1);
        let d2 = doc(2);
        primary.insert_version(&d1, 11, Some(b"a")).unwrap();
        primary.insert_version(&d2, 12, Some(b"b")).unwrap();

        let wal_commits = vec![
            WalCommitInfo {
                commit_ts: 11,
                mutations: vec![(CollectionId(1), d1)],
                index_deltas: vec![],
            },
            WalCommitInfo {
                commit_ts: 12,
                mutations: vec![(CollectionId(1), d2)],
                index_deltas: vec![],
            },
        ];

        let count =
            RollbackVacuum::rollback_from_wal(10, &wal_commits, &primaries, &secondaries).unwrap();
        assert_eq!(count, 2);
        assert!(primary.get_at_ts(&d1, 15).unwrap().is_none());
        assert!(primary.get_at_ts(&d2, 15).unwrap().is_none());
    }
}
