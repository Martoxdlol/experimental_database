use dioxus::prelude::*;

use crate::state::AppState;

#[component]
pub fn DocumentsModule() -> Element {
    let state = use_context::<AppState>();
    let db = state.db.read().clone();

    let Some(db) = db else {
        return rsx! { div { class: "empty-state", "No database open" } };
    };

    let mut read_ts_input: Signal<String> = use_signal(|| "max".to_string());
    let mut selected_collection: Signal<Option<(u64, String, u32)>> = use_signal(|| None);
    let mut selected_doc: Signal<Option<String>> = use_signal(|| None);

    // Load collections from catalog
    let collections = use_resource(move || {
        let _rev = *state.revision.read();
        let db = db.clone();
        async move {
            tokio::task::spawn_blocking(move || db.list_collections())
                .await
                .ok()
                .and_then(|r| r.ok())
        }
    });

    let read_ts: u64 = {
        let s = read_ts_input.read();
        if s.as_str() == "max" || s.is_empty() {
            u64::MAX
        } else {
            s.parse().unwrap_or(u64::MAX)
        }
    };

    rsx! {
        div { class: "master-detail",
            // Left panel: collection list + read_ts
            div { class: "master-panel",
                div { style: "padding: 8px;",
                    div { style: "font-weight: bold; margin-bottom: 8px; font-size: 12px;",
                        "Collections"
                    }
                    div { style: "margin-bottom: 8px;",
                        label { style: "color: var(--text-secondary); font-size: 10px; display: block;",
                            "read_ts:"
                        }
                        input {
                            class: "filter-input",
                            style: "margin: 2px 0;",
                            value: "{read_ts_input.read()}",
                            oninput: move |e| read_ts_input.set(e.value()),
                        }
                    }
                }
                if let Some(Some(cols)) = collections.read().as_ref() {
                    for col in cols {
                        {
                            let col_id = col.id;
                            let col_name = col.name.clone();
                            let root_page = col.data_root_page;
                            let sel = selected_collection.read().as_ref().map(|s| s.0) == Some(col_id);
                            rsx! {
                                div {
                                    class: if sel { "list-item selected" } else { "list-item" },
                                    onclick: move |_| {
                                        selected_collection.set(Some((col_id, col_name.clone(), root_page)));
                                        selected_doc.set(None);
                                    },
                                    span { style: "flex: 1;", "{col.name}" }
                                    span { style: "color: var(--text-secondary); font-size: 10px;",
                                        "pg#{root_page}"
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Right panel: document browser
            div { class: "detail-panel",
                if let Some((_col_id, col_name, root_page)) = selected_collection.read().clone() {
                    DocumentBrowser {
                        collection_name: col_name,
                        root_page,
                        read_ts,
                        selected_doc,
                    }
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
fn DocumentBrowser(
    collection_name: String,
    root_page: u32,
    read_ts: u64,
    selected_doc: Signal<Option<String>>,
) -> Element {
    let state = use_context::<AppState>();
    let db = state.db.read().clone();

    let Some(db) = db else {
        return rsx! { "No database" };
    };

    // MVCC scan
    let documents = use_resource(move || {
        let _rev = *state.revision.read();
        let db = db.clone();
        async move {
            tokio::task::spawn_blocking(move || db.docstore_scan(root_page, read_ts, 200))
                .await
                .ok()
                .and_then(|r| r.ok())
        }
    });

    let ts_label = if read_ts == u64::MAX {
        "latest".to_string()
    } else {
        format!("ts={read_ts}")
    };

    rsx! {
        h3 { style: "margin-bottom: 4px;", "{collection_name}" }
        div { style: "color: var(--text-secondary); font-size: 11px; margin-bottom: 12px;",
            "MVCC scan @ {ts_label}"
        }

        if let Some(Some(docs)) = documents.read().as_ref() {
            div { style: "color: var(--text-secondary); font-size: 11px; margin-bottom: 12px;",
                "{docs.len()} visible documents"
            }
            for doc in docs {
                {
                    let doc_id_hex = doc.doc_id_hex.clone();
                    let is_selected = *selected_doc.read() == Some(doc_id_hex.clone());
                    rsx! {
                        div { class: "card",
                            div { class: "card-header",
                                span { style: "font-size: 11px; flex: 1; font-family: monospace;",
                                    "{short_id(&doc.doc_id_hex)}"
                                }
                                span { style: "color: var(--text-secondary); font-size: 10px; margin-left: 8px;",
                                    "ts={doc.version_ts}"
                                }
                                if doc.is_tombstone {
                                    span { class: "tombstone-badge", "TOMBSTONE" }
                                }
                                if doc.is_external {
                                    span { class: "external-badge", "HEAP" }
                                }
                            }
                            div { class: "card-body",
                                if let Some(json) = &doc.body_json {
                                    pre {
                                        style: "font-size: 12px; white-space: pre-wrap; color: var(--text-primary); max-height: 200px; overflow: auto;",
                                        "{json}"
                                    }
                                } else if doc.is_tombstone {
                                    div { style: "color: var(--page-deleted); font-size: 12px; font-style: italic;",
                                        "(deleted)"
                                    }
                                } else {
                                    div { style: "color: var(--text-secondary); font-size: 11px;",
                                        "{doc.body_len} bytes (binary)"
                                    }
                                }
                            }
                            // Version history (expandable)
                            if is_selected {
                                VersionHistory { root_page, doc_id_hex: doc.doc_id_hex.clone() }
                            } else {
                                div {
                                    style: "padding: 4px 16px 8px; font-size: 10px; color: var(--text-secondary); cursor: pointer;",
                                    onclick: {
                                        let did = doc.doc_id_hex.clone();
                                        move |_| selected_doc.set(Some(did.clone()))
                                    },
                                    "Show version history..."
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

#[component]
fn VersionHistory(root_page: u32, doc_id_hex: String) -> Element {
    let state = use_context::<AppState>();
    let db = state.db.read().clone();

    let Some(db) = db else {
        return rsx! { "No database" };
    };

    let doc_id_for_fetch = doc_id_hex.clone();
    let versions = use_resource(move || {
        let db = db.clone();
        let did = doc_id_for_fetch.clone();
        async move {
            tokio::task::spawn_blocking(move || db.docstore_versions(root_page, &did))
                .await
                .ok()
                .and_then(|r| r.ok())
        }
    });

    rsx! {
        div { style: "padding: 8px 16px; border-top: 1px solid var(--border);",
            div { style: "font-size: 11px; font-weight: bold; margin-bottom: 6px; color: var(--text-secondary);",
                "Version History"
            }
            if let Some(Some(vers)) = versions.read().as_ref() {
                for v in vers {
                    div {
                        style: "display: flex; align-items: center; gap: 8px; padding: 3px 0; border-bottom: 1px solid rgba(58,58,80,0.2);",
                        span { style: "font-size: 11px; color: var(--accent); min-width: 70px;",
                            "ts={v.ts}"
                        }
                        if v.is_tombstone {
                            span { class: "tombstone-badge", "DEL" }
                        }
                        span { style: "font-size: 11px; color: var(--text-secondary); flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;",
                            "{v.body_preview}"
                        }
                    }
                }
            } else {
                div { style: "font-size: 11px; color: var(--text-secondary);", "Loading..." }
            }
        }
    }
}

fn short_id(hex: &str) -> String {
    if hex.len() > 12 {
        format!("{}...{}", &hex[..8], &hex[hex.len() - 4..])
    } else {
        hex.to_string()
    }
}
