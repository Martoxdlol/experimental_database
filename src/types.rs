use std::fmt;

pub type CollectionId = u64;
pub type IndexId = u64;
pub type TxId = u64;
pub type Ts = u64; // commit timestamp, monotonic on primary
pub type SubId = u64;

/// Fixed 16-byte document identifier.
/// Layout: 8 bytes timestamp micros || 8 bytes atomic counter
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DocId([u8; 16]);

impl DocId {
    pub fn new(ts_micros: u64, counter: u64) -> Self {
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&ts_micros.to_be_bytes());
        bytes[8..].copy_from_slice(&counter.to_be_bytes());
        DocId(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        DocId(bytes)
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }
}

impl fmt::Debug for DocId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DocId({:032x})", u128::from_be_bytes(self.0))
    }
}

impl fmt::Display for DocId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}", u128::from_be_bytes(self.0))
    }
}

/// Dot-separated field path, e.g. "user.age" -> ["user", "age"]
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct FieldPath(pub Vec<String>);

impl FieldPath {
    pub fn parse(s: &str) -> Self {
        FieldPath(s.split('.').map(|p| p.to_string()).collect())
    }

    pub fn single(field: &str) -> Self {
        FieldPath(vec![field.to_string()])
    }
}

/// A JSON scalar value used in filter predicates and index keys.
#[derive(Clone, Debug, PartialEq)]
pub enum JsonScalar {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    String(String),
}

/// Filter AST for querying documents.
#[derive(Clone, Debug)]
pub enum Filter {
    Eq(FieldPath, JsonScalar),
    Ne(FieldPath, JsonScalar),
    Lt(FieldPath, JsonScalar),
    Lte(FieldPath, JsonScalar),
    Gt(FieldPath, JsonScalar),
    Gte(FieldPath, JsonScalar),
    In(FieldPath, Vec<JsonScalar>),
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
}

/// Specification for a secondary index.
#[derive(Clone, Debug)]
pub struct IndexSpec {
    pub field: FieldPath,
    pub unique: bool,
}

/// State of an index during its lifecycle.
#[derive(Clone, Debug)]
pub enum IndexState {
    Building { started_at_ts: Ts },
    Ready { ready_at_ts: Ts },
    Failed { message: String },
}

/// WAL fsync policy.
#[derive(Clone, Debug)]
pub enum WalFsyncPolicy {
    Always,
    GroupCommit {
        interval_ms: u64,
        max_batch_bytes: usize,
    },
    Never,
}

/// Replication configuration.
#[derive(Clone, Debug, Default)]
pub struct ReplicationConfig {
    pub role: ReplicationRole,
    pub primary_addr: Option<String>,
    pub listen_addr: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub enum ReplicationRole {
    #[default]
    Standalone,
    Primary,
    Replica,
}

/// Patch operation for updating documents.
#[derive(Clone, Debug)]
pub enum PatchOp {
    MergePatch(serde_json::Value),
}

/// Type of query operation.
#[derive(Clone, Debug)]
pub enum QueryType {
    Find,
    FindIds,
}

/// Options for a query session.
#[derive(Clone, Debug, Default)]
pub struct QueryOptions {
    pub subscribe: bool,
    pub limit: Option<usize>,
}

/// Outcome of committing a query session.
pub enum QueryCommitOutcome {
    NotSubscribed,
    Subscribed {
        subscription: crate::subs::Subscription,
    },
    InvalidatedDuringRun,
}

/// Outcome of committing a mutation transaction.
pub enum MutationCommitOutcome {
    Committed { commit_ts: Ts },
    Conflict,
}

/// An invalidation event sent to subscribers.
#[derive(Clone, Debug)]
pub struct InvalidationEvent {
    pub subscription_id: SubId,
    pub invalidated_at_ts: Ts,
}

/// Key range for index scans and read set tracking.
#[derive(Clone, Debug)]
pub struct KeyRange {
    pub start: Vec<u8>, // inclusive
    pub end: Vec<u8>,   // exclusive
}

impl KeyRange {
    pub fn contains(&self, key: &[u8]) -> bool {
        key >= self.start.as_slice() && key < self.end.as_slice()
    }
}
