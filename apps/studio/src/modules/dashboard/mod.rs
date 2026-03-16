use dioxus::prelude::*;

use crate::state::{AppState, NavSection};

#[component]
pub fn DashboardModule() -> Element {
    let mut state = use_context::<AppState>();
    let _rev = *state.revision.read();
    let engine = state.engine.read().clone();

    let info = use_resource(move || {
        let engine = engine.clone();
        async move {
            let Some(engine) = engine else {
                return DashboardInfo::default();
            };
            let collections = engine.l6.list_collections();
            let total_docs: u64 = collections.iter().map(|c| c.doc_count).sum();
            let config = engine.l6.config();
            let fh = engine.storage.read_file_header().await;

            DashboardInfo {
                db_name: engine.l6.name().to_string(),
                db_path: engine.path.clone(),
                collection_count: collections.len(),
                total_docs,
                page_size: config.page_size,
                memory_budget_mb: config.memory_budget / (1024 * 1024),
                page_count: fh.page_count.get(),
                wal_lsn: engine.storage.wal_info().current_lsn,
                collections,
            }
        }
    });

    let info = info.read();
    let info = info.as_ref();

    match info {
        None => rsx! {
            div { class: "main-content",
                div { class: "card", "Loading..." }
            }
        },
        Some(info) => rsx! {
            div { class: "main-content",
                div { class: "card",
                    h3 { "Database Overview" }
                    table { class: "kv-table",
                        tbody {
                            tr { td { "Name" } td { "{info.db_name}" } }
                            tr { td { "Path" } td { class: "mono", "{info.db_path}" } }
                            tr { td { "Collections" } td { "{info.collection_count}" } }
                            tr { td { "Total Documents" } td { "{info.total_docs}" } }
                            tr { td { "Page Size" } td { "{info.page_size} B" } }
                            tr { td { "Memory Budget" } td { "{info.memory_budget_mb} MB" } }
                            tr { td { "Pages" } td { "{info.page_count}" } }
                            tr { td { "WAL LSN" } td { "{info.wal_lsn}" } }
                        }
                    }
                }

                div { class: "card",
                    h3 { "Collections" }
                    if info.collections.is_empty() {
                        div { class: "empty-state",
                            div { "No collections yet" }
                            div { style: "color: var(--text-secondary); font-size: 12px; margin-top: 4px;",
                                "Go to Collections to create one"
                            }
                        }
                    } else {
                        table { class: "data-table",
                            thead {
                                tr {
                                    th { "Name" }
                                    th { "Documents" }
                                    th { "" }
                                }
                            }
                            tbody {
                                for col in &info.collections {
                                    {
                                        let name = col.name.clone();
                                        let name2 = name.clone();
                                        rsx! {
                                            tr {
                                                td { class: "mono", "{name}" }
                                                td { "{col.doc_count}" }
                                                td {
                                                    button {
                                                        class: "btn btn-small",
                                                        onclick: move |_| {
                                                            state.selected_collection.set(Some(name2.clone()));
                                                            state.nav.set(NavSection::Collections);
                                                            state.breadcrumb.set(vec![
                                                                "Database".to_string(),
                                                                "Collections".to_string(),
                                                                name2.clone(),
                                                            ]);
                                                        },
                                                        "Open"
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                div { class: "card",
                    h3 { "Quick Actions" }
                    div { class: "toolbar-group",
                        button {
                            class: "btn",
                            onclick: move |_| {
                                state.nav.set(NavSection::Collections);
                                state.breadcrumb.set(vec![
                                    "Database".to_string(),
                                    "Collections".to_string(),
                                ]);
                            },
                            "Manage Collections"
                        }
                        button {
                            class: "btn",
                            onclick: move |_| {
                                state.nav.set(NavSection::QueryWorkbench);
                                state.breadcrumb.set(vec![
                                    "Database".to_string(),
                                    "Query Workbench".to_string(),
                                ]);
                            },
                            "Query Workbench"
                        }
                        button {
                            class: "btn",
                            onclick: move |_| {
                                state.nav.set(NavSection::Config);
                                state.breadcrumb.set(vec![
                                    "Database".to_string(),
                                    "Configuration".to_string(),
                                ]);
                            },
                            "View Config"
                        }
                    }
                }
            }
        },
    }
}

#[derive(Default)]
struct DashboardInfo {
    db_name: String,
    db_path: String,
    collection_count: usize,
    total_docs: u64,
    page_size: usize,
    memory_budget_mb: usize,
    page_count: u64,
    wal_lsn: u64,
    collections: Vec<exdb::CollectionMeta>,
}
