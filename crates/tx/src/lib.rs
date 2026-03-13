//! exdb-tx — Layer 5: Transaction Manager
//!
//! Transaction lifecycle, timestamp allocation, OCC validation,
//! commit coordination, and live query subscriptions.

pub mod timestamp;
pub mod read_set;
pub mod write_set;
pub mod commit_log;
pub mod occ;
pub mod subscriptions;
pub mod commit;

pub use timestamp::TsAllocator;
pub use read_set::{ReadSet, ReadInterval, CatalogRead};
pub use write_set::{WriteSet, MutationEntry, MutationOp, CatalogMutation, IndexDelta};
pub use commit_log::{CommitLog, CommitLogEntry, IndexKeyWrite};
pub use occ::{validate, ConflictError, ConflictKind};
pub use subscriptions::{SubscriptionRegistry, SubscriptionMode, SubscriptionId, TxId};
pub use commit::{CommitCoordinator, CommitHandle, CommitRequest, CommitResult, ReplicationHook, CatalogMutator};
