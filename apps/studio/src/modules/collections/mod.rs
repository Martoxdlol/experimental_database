use dioxus::prelude::*;

use crate::state::{AppState, OperationResult};

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
        let _rev = *state.revision.read();
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
    let mut state = use_context::<AppState>();
    let db = state.db.read().clone();

    let Some(db) = db else {
        return rsx! { "No database" };
    };

    // Scan the collection's data B-tree
    let documents = use_resource(move || {
        let _rev = *state.revision.read();
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

        // Insert document form (write mode)
        if write_enabled {
            InsertDocumentForm { root_page }
        }

        if let Some(Some(docs)) = documents.read().as_ref() {
            div { style: "color: var(--text-secondary); font-size: 11px; margin-bottom: 12px;",
                "{docs.len()} entries (raw B-tree scan)"
            }
            for (key, value) in docs {
                {
                    let key_clone = key.clone();
                    let value_clone = value.clone();
                    rsx! {
                        div { class: "card",
                            div { class: "card-header",
                                span { style: "font-size: 11px; color: var(--text-secondary); flex: 1;",
                                    "key: {hex_short(key)}"
                                }
                                if write_enabled {
                                    button {
                                        class: "edit-btn",
                                        title: "Delete entry",
                                        onclick: move |e| {
                                            e.stop_propagation();
                                            let db = state.db.read().clone();
                                            if let Some(db) = db {
                                                match db.btree_delete(root_page, &key_clone) {
                                                    Ok(true) => {
                                                        state.last_result.set(Some(OperationResult::Success("Entry deleted".into())));
                                                        state.notify_mutation();
                                                    }
                                                    Ok(false) => state.last_result.set(Some(OperationResult::Error("Key not found".into()))),
                                                    Err(e) => state.last_result.set(Some(OperationResult::Error(e.to_string()))),
                                                }
                                            }
                                        },
                                        "\u{2717}"
                                    }
                                }
                            }
                            div { class: "card-body",
                                // Try to parse value as JSON
                                {
                                    if let Ok(text) = std::str::from_utf8(&value_clone) {
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
                                                "{value_clone.len()} bytes (binary)"
                                            }
                                            div { style: "font-size: 11px;",
                                                "{hex_short(&value_clone)}"
                                            }
                                        }
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

#[component]
fn InsertDocumentForm(root_page: u32) -> Element {
    let mut state = use_context::<AppState>();
    let mut key_hex: Signal<String> = use_signal(String::new);
    let mut value_input: Signal<String> = use_signal(String::new);
    let mut value_mode: Signal<String> = use_signal(|| "json".to_string());

    rsx! {
        div { class: "card",
            div { class: "card-header", "Insert Document" }
            div { class: "card-body",
                div {
                    label { style: "color: var(--text-secondary); font-size: 11px; display: block;", "Key (hex):" }
                    input {
                        class: "edit-input",
                        style: "width: 100%; margin-bottom: 8px;",
                        placeholder: "e.g. doc_id hex bytes",
                        value: "{key_hex.read()}",
                        oninput: move |e| key_hex.set(e.value()),
                    }
                }
                div { style: "display: flex; gap: 8px; margin-bottom: 8px;",
                    label { style: "font-size: 11px; color: var(--text-secondary);",
                        input {
                            r#type: "radio",
                            name: "doc_mode_{root_page}",
                            checked: *value_mode.read() == "json",
                            onchange: move |_| value_mode.set("json".to_string()),
                        }
                        " JSON"
                    }
                    label { style: "font-size: 11px; color: var(--text-secondary);",
                        input {
                            r#type: "radio",
                            name: "doc_mode_{root_page}",
                            checked: *value_mode.read() == "hex",
                            onchange: move |_| value_mode.set("hex".to_string()),
                        }
                        " Hex"
                    }
                }
                div {
                    label { style: "color: var(--text-secondary); font-size: 11px; display: block;",
                        "Value ({value_mode.read()}):"
                    }
                    textarea {
                        class: "edit-input",
                        style: "width: 100%; height: 100px; font-family: monospace; resize: vertical;",
                        placeholder: if *value_mode.read() == "json" { "JSON object" } else { "hex bytes" },
                        value: "{value_input.read()}",
                        oninput: move |e| value_input.set(e.value()),
                    }
                }
                button {
                    class: "btn btn-action",
                    style: "margin-top: 8px;",
                    onclick: move |_| {
                        let key = parse_hex(&key_hex.read());
                        let value = if *value_mode.read() == "json" {
                            // Validate JSON and store as UTF-8 bytes
                            let text = value_input.read().clone();
                            match serde_json::from_str::<serde_json::Value>(&text) {
                                Ok(_) => Some(text.into_bytes()),
                                Err(e) => {
                                    state.last_result.set(Some(OperationResult::Error(format!("Invalid JSON: {e}"))));
                                    return;
                                }
                            }
                        } else {
                            parse_hex(&value_input.read())
                        };
                        if let (Some(k), Some(v)) = (key, value) {
                            if k.is_empty() {
                                state.last_result.set(Some(OperationResult::Error("Key required".into())));
                                return;
                            }
                            let db = state.db.read().clone();
                            if let Some(db) = db {
                                match db.btree_insert(root_page, &k, &v) {
                                    Ok(()) => {
                                        state.last_result.set(Some(OperationResult::Success("Document inserted".into())));
                                        state.notify_mutation();
                                        key_hex.set(String::new());
                                        value_input.set(String::new());
                                    }
                                    Err(e) => state.last_result.set(Some(OperationResult::Error(e.to_string()))),
                                }
                            }
                        } else {
                            state.last_result.set(Some(OperationResult::Error("Invalid hex input".into())));
                        }
                    },
                    "Insert"
                }
            }
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

fn parse_hex(s: &str) -> Option<Vec<u8>> {
    let s = s.replace(' ', "");
    if s.is_empty() {
        return Some(Vec::new());
    }
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}
