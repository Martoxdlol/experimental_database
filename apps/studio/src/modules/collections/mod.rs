use dioxus::prelude::*;

use crate::state::AppState;

#[component]
pub fn CollectionsModule() -> Element {
    let state = use_context::<AppState>();
    let db = state.db.read().clone();
    let write_enabled = *state.write_enabled.read();

    let Some(db) = db else {
        return rsx! { div { class: "empty-state", "No database open" } };
    };

    let mut selected_collection: Signal<Option<(u64, String, u32)>> = use_signal(|| None);

    let collections = use_resource(move || {
        let db = db.clone();
        async move {
            tokio::task::spawn_blocking(move || db.list_collections())
                .await
                .ok()
                .and_then(|r| r.ok())
        }
    });

    rsx! {
        div { class: "master-detail",
            // Collection list (left panel)
            div { class: "master-panel",
                div { style: "padding: 8px;",
                    div { style: "font-weight: bold; margin-bottom: 8px; font-size: 12px;",
                        "Collections"
                    }
                }
                if let Some(Some(cols)) = collections.read().as_ref() {
                    for col in cols {
                        {
                            let col_id = col.id;
                            let col_name = col.name.clone();
                            let root_page = col.data_root_page;
                            let doc_count = col.doc_count;
                            let sel = selected_collection.read().as_ref().map(|s| s.0) == Some(col_id);
                            rsx! {
                                div {
                                    class: if sel { "list-item selected" } else { "list-item" },
                                    onclick: move |_| {
                                        selected_collection.set(Some((col_id, col_name.clone(), root_page)));
                                    },
                                    span { style: "flex: 1;", "{col.name}" }
                                    span { style: "color: var(--text-secondary); font-size: 10px;", "({doc_count})" }
                                }
                            }
                        }
                    }
                }
            }

            // Document view (right panel)
            div { class: "detail-panel",
                if let Some((_col_id, col_name, root_page)) = selected_collection.read().clone() {
                    DocumentBrowser { collection_name: col_name, root_page, write_enabled }
                } else {
                    div { class: "empty-state",
                        div { class: "empty-state-icon", "\u{1F4C1}" }
                        "Select a collection to browse documents"
                    }
                }
            }
        }
    }
}

#[component]
fn DocumentBrowser(collection_name: String, root_page: u32, write_enabled: bool) -> Element {
    let state = use_context::<AppState>();
    let db = state.db.read().clone();

    let Some(db) = db else {
        return rsx! { "No database" };
    };

    // Scan the collection's data B-tree
    let documents = use_resource(move || {
        let db = db.clone();
        async move {
            tokio::task::spawn_blocking(move || db.btree_scan(root_page, 100))
                .await
                .ok()
                .and_then(|r| r.ok())
        }
    });

    rsx! {
        h3 { style: "margin-bottom: 12px;",
            "{collection_name}"
        }

        if let Some(Some(docs)) = documents.read().as_ref() {
            div { style: "color: var(--text-secondary); font-size: 11px; margin-bottom: 12px;",
                "{docs.len()} entries (raw B-tree scan)"
            }
            for (key, value) in docs {
                div { class: "card",
                    div { class: "card-header",
                        span { style: "font-size: 11px; color: var(--text-secondary);",
                            "key: {hex_short(key)}"
                        }
                    }
                    div { class: "card-body",
                        // Try to parse value as JSON
                        {
                            if let Ok(text) = std::str::from_utf8(value) {
                                if let Ok(json) = serde_json::from_str::<serde_json::Value>(text) {
                                    let pretty = serde_json::to_string_pretty(&json).unwrap_or_default();
                                    rsx! {
                                        pre {
                                            style: "font-size: 12px; white-space: pre-wrap; color: var(--text-primary);",
                                            "{pretty}"
                                        }
                                    }
                                } else {
                                    rsx! {
                                        pre { style: "font-size: 12px;", "{text}" }
                                    }
                                }
                            } else {
                                rsx! {
                                    div { style: "font-size: 11px; color: var(--text-secondary);",
                                        "{value.len()} bytes (binary)"
                                    }
                                    div { style: "font-size: 11px;",
                                        "{hex_short(value)}"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } else {
            div { "Loading documents..." }
        }
    }
}

fn hex_short(data: &[u8]) -> String {
    if data.len() <= 16 {
        data.iter().map(|b| format!("{b:02X}")).collect::<Vec<_>>().join(" ")
    } else {
        let head: String = data[..8].iter().map(|b| format!("{b:02X}")).collect::<Vec<_>>().join(" ");
        format!("{head}... ({} bytes)", data.len())
    }
}
