use dioxus::prelude::*;

use crate::components::{Breadcrumb, NavSidebar, Toast, Toolbar};
use crate::modules::{
    collections::CollectionsModule,
    config_viewer::ConfigViewerModule,
    dashboard::DashboardModule,
    internals::{
        console::ConsoleModule,
        docstore::{DocumentsModule, KeyToolsModule, SecondaryIndexModule, VacuumModule},
        query::{FilterTestModule, QueryScanModule, RangeToolsModule},
        storage::{
            BTreeModule, CatalogModule, FreeListModule, HeapModule, PagesModule, WalModule,
        },
    },
    query_workbench::QueryWorkbenchModule,
    subscriptions::SubscriptionsModule,
};
use crate::state::{AppState, InternalsTool, NavSection};
use crate::theme::STYLESHEET;

#[component]
pub fn App() -> Element {
    let state = AppState::new();
    use_context_provider(|| state);

    rsx! {
        style { "{STYLESHEET}" }
        div { class: "app-container",
            Toolbar {}
            div { class: "main-layout",
                NavSidebar {}
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
    let nav = *state.nav.read();
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

    match nav {
        NavSection::Dashboard => rsx! { DashboardModule {} },
        NavSection::Collections => rsx! { CollectionsModule {} },
        NavSection::QueryWorkbench => rsx! { QueryWorkbenchModule {} },
        NavSection::Subscriptions => rsx! { SubscriptionsModule {} },
        NavSection::Config => rsx! { ConfigViewerModule {} },
        NavSection::Internals(tool) => match tool {
            InternalsTool::StoragePages => rsx! { PagesModule {} },
            InternalsTool::StorageBTree => rsx! { BTreeModule {} },
            InternalsTool::StorageWal => rsx! { WalModule {} },
            InternalsTool::StorageHeap => rsx! { HeapModule {} },
            InternalsTool::StorageFreeList => rsx! { FreeListModule {} },
            InternalsTool::StorageCatalog => rsx! { CatalogModule {} },
            InternalsTool::DocstoreDocs => rsx! { DocumentsModule {} },
            InternalsTool::DocstoreSecondary => rsx! { SecondaryIndexModule {} },
            InternalsTool::DocstoreKeys => rsx! { KeyToolsModule {} },
            InternalsTool::DocstoreVacuum => rsx! { VacuumModule {} },
            InternalsTool::QueryScan => rsx! { QueryScanModule {} },
            InternalsTool::QueryFilter => rsx! { FilterTestModule {} },
            InternalsTool::QueryRange => rsx! { RangeToolsModule {} },
            InternalsTool::Console => rsx! { ConsoleModule {} },
        },
    }
}
