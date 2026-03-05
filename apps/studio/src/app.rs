use dioxus::prelude::*;

use crate::components::{Breadcrumb, Sidebar, Toast, Toolbar};
use crate::modules::{
    btree::BTreeModule, catalog::CatalogModule, collections::CollectionsModule,
    console::ConsoleModule, freelist::FreeListModule, heap::HeapModule, overview::OverviewModule,
    pages::PagesModule, wal::WalModule,
};
use crate::state::{AppState, Module};
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
    let active = *state.active_module.read();
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

    match active {
        Module::Overview => rsx! { OverviewModule {} },
        Module::Collections => rsx! { CollectionsModule {} },
        Module::Pages => rsx! { PagesModule {} },
        Module::BTree => rsx! { BTreeModule {} },
        Module::Wal => rsx! { WalModule {} },
        Module::Heap => rsx! { HeapModule {} },
        Module::FreeList => rsx! { FreeListModule {} },
        Module::Catalog => rsx! { CatalogModule {} },
        Module::Console => rsx! { ConsoleModule {} },
    }
}
