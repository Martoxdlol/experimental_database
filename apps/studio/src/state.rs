use std::sync::Arc;

use dioxus::prelude::*;

use crate::engine::DbHandle;

/// Which module is currently active in the sidebar.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Module {
    Overview,
    Collections,
    Pages,
    BTree,
    Wal,
    Heap,
    FreeList,
    Catalog,
    Console,
}

impl Module {
    pub fn label(&self) -> &'static str {
        match self {
            Module::Overview => "Ovw",
            Module::Collections => "Cols",
            Module::Pages => "Pgs",
            Module::BTree => "BTree",
            Module::Wal => "WAL",
            Module::Heap => "Heap",
            Module::FreeList => "Free",
            Module::Catalog => "Cat",
            Module::Console => "Cons",
        }
    }

    pub fn icon(&self) -> &'static str {
        match self {
            Module::Overview => "\u{1F4CA}",   // bar chart
            Module::Collections => "\u{1F4C1}", // folder
            Module::Pages => "\u{1F4C4}",      // page
            Module::BTree => "\u{1F333}",      // tree
            Module::Wal => "\u{1F4DD}",        // memo
            Module::Heap => "\u{1F4E6}",       // package
            Module::FreeList => "\u{1F517}",   // link
            Module::Catalog => "\u{1F4D6}",    // book
            Module::Console => "\u{1F4BB}",    // laptop
        }
    }

    pub const ALL: &[Module] = &[
        Module::Overview,
        Module::Collections,
        Module::Pages,
        Module::BTree,
        Module::Wal,
        Module::Heap,
        Module::FreeList,
        Module::Catalog,
        Module::Console,
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
    /// Active sidebar module.
    pub active_module: Signal<Module>,
    /// Breadcrumb trail.
    pub breadcrumb: Signal<Vec<String>>,
    /// Read-write lock. false = locked (read-only), true = writes allowed.
    pub write_enabled: Signal<bool>,
    /// Dirty page count from buffer pool.
    pub dirty_page_count: Signal<usize>,
    /// Last operation result for toast display.
    pub last_result: Signal<Option<OperationResult>>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            db: Signal::new(None),
            active_module: Signal::new(Module::Overview),
            breadcrumb: Signal::new(vec!["Database".to_string()]),
            write_enabled: Signal::new(false),
            dirty_page_count: Signal::new(0),
            last_result: Signal::new(None),
        }
    }

    /// Whether a database is currently open.
    pub fn is_open(&self) -> bool {
        self.db.read().is_some()
    }
}
