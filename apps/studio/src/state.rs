use std::sync::Arc;

use dioxus::prelude::*;

use crate::engine::{EngineHandle, StorageHandle};

// ─── Navigation Model ───

/// Top-level navigation section.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NavSection {
    Dashboard,
    Collections,
    QueryWorkbench,
    Subscriptions,
    Config,
    Internals(InternalsTool),
}

/// Internals sub-navigation (L2-L4 tools).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InternalsTool {
    // Storage (L2)
    StoragePages,
    StorageBTree,
    StorageWal,
    StorageHeap,
    StorageFreeList,
    StorageCatalog,
    // Docstore (L3)
    DocstoreDocs,
    DocstoreSecondary,
    DocstoreKeys,
    DocstoreVacuum,
    // Query (L4)
    QueryScan,
    QueryFilter,
    QueryRange,
    // Console
    Console,
}

impl InternalsTool {
    pub fn label(&self) -> &'static str {
        match self {
            Self::StoragePages => "Pages",
            Self::StorageBTree => "B-Trees",
            Self::StorageWal => "WAL",
            Self::StorageHeap => "Heap",
            Self::StorageFreeList => "Free List",
            Self::StorageCatalog => "Catalog",
            Self::DocstoreDocs => "Documents",
            Self::DocstoreSecondary => "Secondary Indexes",
            Self::DocstoreKeys => "Key Tools",
            Self::DocstoreVacuum => "Vacuum",
            Self::QueryScan => "Scan",
            Self::QueryFilter => "Filter",
            Self::QueryRange => "Range",
            Self::Console => "Console",
        }
    }

    pub fn group(&self) -> &'static str {
        match self {
            Self::StoragePages
            | Self::StorageBTree
            | Self::StorageWal
            | Self::StorageHeap
            | Self::StorageFreeList
            | Self::StorageCatalog => "Storage",
            Self::DocstoreDocs
            | Self::DocstoreSecondary
            | Self::DocstoreKeys
            | Self::DocstoreVacuum => "Docstore",
            Self::QueryScan | Self::QueryFilter | Self::QueryRange => "Query Engine",
            Self::Console => "Console",
        }
    }

    pub const STORAGE: &[InternalsTool] = &[
        Self::StoragePages,
        Self::StorageBTree,
        Self::StorageWal,
        Self::StorageHeap,
        Self::StorageFreeList,
        Self::StorageCatalog,
    ];

    pub const DOCSTORE: &[InternalsTool] = &[
        Self::DocstoreDocs,
        Self::DocstoreSecondary,
        Self::DocstoreKeys,
        Self::DocstoreVacuum,
    ];

    pub const QUERY: &[InternalsTool] = &[
        Self::QueryScan,
        Self::QueryFilter,
        Self::QueryRange,
    ];
}

/// Sub-view within a collection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CollectionView {
    Documents,
    Indexes,
}

/// Operation result for toast notifications.
#[derive(Clone, Debug)]
pub enum OperationResult {
    Success(String),
    Error(String),
}

/// Global application state shared via Dioxus context.
#[derive(Clone, Copy)]
pub struct AppState {
    /// Currently open engine handle (L6 + storage).
    pub engine: Signal<Option<Arc<EngineHandle>>>,
    /// Backward-compatible storage handle for internals modules.
    /// Set alongside `engine` on open, cleared on close.
    pub db: Signal<Option<Arc<StorageHandle>>>,
    /// Active navigation section.
    pub nav: Signal<NavSection>,
    /// Selected collection name (for Collections view).
    pub selected_collection: Signal<Option<String>>,
    /// Sub-view within a collection (Documents or Indexes).
    pub collection_view: Signal<CollectionView>,
    /// Whether the Internals section is expanded in the sidebar.
    pub internals_expanded: Signal<bool>,
    /// Breadcrumb trail.
    pub breadcrumb: Signal<Vec<String>>,
    /// Read-write lock for internals. false = locked, true = writes allowed.
    pub write_enabled: Signal<bool>,
    /// Dirty page count from buffer pool.
    pub dirty_page_count: Signal<usize>,
    /// Last operation result for toast display.
    pub last_result: Signal<Option<OperationResult>>,
    /// Monotonic revision counter -- bumped on every write mutation.
    pub revision: Signal<u64>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            engine: Signal::new(None),
            db: Signal::new(None),
            nav: Signal::new(NavSection::Dashboard),
            selected_collection: Signal::new(None),
            collection_view: Signal::new(CollectionView::Documents),
            internals_expanded: Signal::new(false),
            breadcrumb: Signal::new(vec!["Database".to_string()]),
            write_enabled: Signal::new(false),
            dirty_page_count: Signal::new(0),
            last_result: Signal::new(None),
            revision: Signal::new(0),
        }
    }

    /// Whether a database is currently open.
    pub fn is_open(&self) -> bool {
        self.engine.read().is_some()
    }

    /// Call after any successful write mutation to refresh all data views.
    pub fn notify_mutation(&mut self) {
        self.revision.set(self.revision.cloned() + 1);
        if let Some(db) = self.db.read().as_ref() {
            self.dirty_page_count.set(db.dirty_page_count());
        }
    }
}
