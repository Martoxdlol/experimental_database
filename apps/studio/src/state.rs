use std::sync::Arc;

use dioxus::prelude::*;

use crate::engine::DbHandle;

/// Top-level layer tabs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LayerTab {
    Overview,
    Storage,   // L2
    Docstore,  // L3
    Query,     // L4
    Console,
}

impl LayerTab {
    pub fn label(&self) -> &'static str {
        match self {
            LayerTab::Overview => "Overview",
            LayerTab::Storage => "L2 Storage",
            LayerTab::Docstore => "L3 Docstore",
            LayerTab::Query => "L4 Query",
            LayerTab::Console => "Console",
        }
    }

    pub const ALL: &[LayerTab] = &[
        LayerTab::Overview,
        LayerTab::Storage,
        LayerTab::Docstore,
        LayerTab::Query,
        LayerTab::Console,
    ];
}

/// Tools within the L2 Storage tab.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageTool {
    Pages,
    BTree,
    Wal,
    Heap,
    FreeList,
    Catalog,
}

impl StorageTool {
    pub fn label(&self) -> &'static str {
        match self {
            StorageTool::Pages => "Pages",
            StorageTool::BTree => "BTree",
            StorageTool::Wal => "WAL",
            StorageTool::Heap => "Heap",
            StorageTool::FreeList => "Free",
            StorageTool::Catalog => "Catalog",
        }
    }

    pub fn icon(&self) -> &'static str {
        match self {
            StorageTool::Pages => "\u{1F4C4}",
            StorageTool::BTree => "\u{1F333}",
            StorageTool::Wal => "\u{1F4DD}",
            StorageTool::Heap => "\u{1F4E6}",
            StorageTool::FreeList => "\u{1F517}",
            StorageTool::Catalog => "\u{1F4D6}",
        }
    }

    pub const ALL: &[StorageTool] = &[
        StorageTool::Pages,
        StorageTool::BTree,
        StorageTool::Wal,
        StorageTool::Heap,
        StorageTool::FreeList,
        StorageTool::Catalog,
    ];
}

/// Tools within the L3 Docstore tab.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DocstoreTool {
    Documents,
    SecondaryIndexes,
    KeyTools,
    Vacuum,
}

impl DocstoreTool {
    pub fn label(&self) -> &'static str {
        match self {
            DocstoreTool::Documents => "Docs",
            DocstoreTool::SecondaryIndexes => "Indexes",
            DocstoreTool::KeyTools => "Keys",
            DocstoreTool::Vacuum => "Vacuum",
        }
    }

    pub fn icon(&self) -> &'static str {
        match self {
            DocstoreTool::Documents => "\u{1F4C1}",
            DocstoreTool::SecondaryIndexes => "\u{1F50D}",
            DocstoreTool::KeyTools => "\u{1F511}",
            DocstoreTool::Vacuum => "\u{1F9F9}",
        }
    }

    pub const ALL: &[DocstoreTool] = &[
        DocstoreTool::Documents,
        DocstoreTool::SecondaryIndexes,
        DocstoreTool::KeyTools,
        DocstoreTool::Vacuum,
    ];
}

/// Tools within the L4 Query tab.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryTool {
    Scan,
    FilterTest,
    RangeTools,
}

impl QueryTool {
    pub fn label(&self) -> &'static str {
        match self {
            QueryTool::Scan => "Scan",
            QueryTool::FilterTest => "Filter",
            QueryTool::RangeTools => "Range",
        }
    }

    pub fn icon(&self) -> &'static str {
        match self {
            QueryTool::Scan => "\u{1F50E}",
            QueryTool::FilterTest => "\u{1F3AF}",
            QueryTool::RangeTools => "\u{1F4CF}",
        }
    }

    pub const ALL: &[QueryTool] = &[
        QueryTool::Scan,
        QueryTool::FilterTest,
        QueryTool::RangeTools,
    ];
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
    /// Currently open database handle.
    pub db: Signal<Option<Arc<DbHandle>>>,
    /// Active layer tab.
    pub active_tab: Signal<LayerTab>,
    /// Active tool within Storage tab.
    pub storage_tool: Signal<StorageTool>,
    /// Active tool within Docstore tab.
    pub docstore_tool: Signal<DocstoreTool>,
    /// Active tool within Query tab.
    pub query_tool: Signal<QueryTool>,
    /// Breadcrumb trail.
    pub breadcrumb: Signal<Vec<String>>,
    /// Read-write lock. false = locked (read-only), true = writes allowed.
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
            db: Signal::new(None),
            active_tab: Signal::new(LayerTab::Overview),
            storage_tool: Signal::new(StorageTool::Pages),
            docstore_tool: Signal::new(DocstoreTool::Documents),
            query_tool: Signal::new(QueryTool::Scan),
            breadcrumb: Signal::new(vec!["Database".to_string()]),
            write_enabled: Signal::new(false),
            dirty_page_count: Signal::new(0),
            last_result: Signal::new(None),
            revision: Signal::new(0),
        }
    }

    /// Whether a database is currently open.
    pub fn is_open(&self) -> bool {
        self.db.read().is_some()
    }

    /// Call after any successful write mutation to refresh all data views.
    pub fn notify_mutation(&mut self) {
        self.revision.set(self.revision.cloned() + 1);
        if let Some(db) = self.db.read().as_ref() {
            self.dirty_page_count.set(db.dirty_page_count());
        }
    }
}
