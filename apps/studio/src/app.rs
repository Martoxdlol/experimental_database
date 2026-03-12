use dioxus::prelude::*;

use crate::components::{Breadcrumb, LayerTabBar, Sidebar, Toast, Toolbar};
use crate::modules::{
    console::ConsoleModule,
    docstore::{DocumentsModule, KeyToolsModule, SecondaryIndexModule, VacuumModule},
    overview::OverviewModule,
    query::{QueryScanModule, FilterTestModule, RangeToolsModule},
    storage::{
        BTreeModule, CatalogModule, FreeListModule, HeapModule, PagesModule, WalModule,
    },
};
use crate::state::{AppState, DocstoreTool, LayerTab, QueryTool, StorageTool};
use crate::theme::STYLESHEET;

#[component]
pub fn App() -> Element {
    let state = AppState::new();
    use_context_provider(|| state);

    rsx! {
        style { "{STYLESHEET}" }
        div { class: "app-container",
            Toolbar {}
            LayerTabBar {}
            div { class: "main-layout",
                Sidebar {}
                div { class: "content",
                    Breadcrumb {}
                    ModuleContent {}
                }
            }
            Toast {}
        }
    }
}

#[component]
fn ModuleContent() -> Element {
    let state = use_context::<AppState>();
    let active_tab = *state.active_tab.read();
    let db_open = state.is_open();

    if !db_open {
        return rsx! {
            div { class: "main-content",
                div { class: "empty-state",
                    div { class: "empty-state-icon", "\u{1F4BE}" }
                    div { "Open a database to get started" }
                    div { style: "color: var(--text-secondary); font-size: 12px; margin-top: 8px;",
                        "Click \"Open DB...\" in the toolbar to select a database directory"
                    }
                }
            }
        };
    }

    match active_tab {
        LayerTab::Overview => rsx! { OverviewModule {} },
        LayerTab::Console => rsx! { ConsoleModule {} },
        LayerTab::Storage => {
            let tool = *state.storage_tool.read();
            match tool {
                StorageTool::Pages => rsx! { PagesModule {} },
                StorageTool::BTree => rsx! { BTreeModule {} },
                StorageTool::Wal => rsx! { WalModule {} },
                StorageTool::Heap => rsx! { HeapModule {} },
                StorageTool::FreeList => rsx! { FreeListModule {} },
                StorageTool::Catalog => rsx! { CatalogModule {} },
            }
        }
        LayerTab::Docstore => {
            let tool = *state.docstore_tool.read();
            match tool {
                DocstoreTool::Documents => rsx! { DocumentsModule {} },
                DocstoreTool::SecondaryIndexes => rsx! { SecondaryIndexModule {} },
                DocstoreTool::KeyTools => rsx! { KeyToolsModule {} },
                DocstoreTool::Vacuum => rsx! { VacuumModule {} },
            }
        }
        LayerTab::Query => {
            let tool = *state.query_tool.read();
            match tool {
                QueryTool::Scan => rsx! { QueryScanModule {} },
                QueryTool::FilterTest => rsx! { FilterTestModule {} },
                QueryTool::RangeTools => rsx! { RangeToolsModule {} },
            }
        }
    }
}
