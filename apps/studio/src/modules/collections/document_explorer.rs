use dioxus::prelude::*;

use exdb::ScanDirection;

use crate::state::{AppState, OperationResult};

#[component]
pub fn DocumentExplorer(collection: String) -> Element {
    let mut state = use_context::<AppState>();
    let _rev = *state.revision.read();

    let mut direction = use_signal(|| ScanDirection::Backward);
    let mut limit = use_signal(|| 50usize);
    let mut selected_doc: Signal<Option<usize>> = use_signal(|| None);
    let mut insert_mode = use_signal(|| false);
    let mut insert_json = use_signal(|| "{\n  \n}".to_string());

    let engine = state.engine.read().clone();
    let coll = collection.clone();
    let dir = *direction.read();
    let lim = *limit.read();

    let docs = use_resource(move || {
        let engine = engine.clone();
        let coll = coll.clone();
        async move {
            let Some(engine) = engine else { return Vec::new() };
            engine.l6.scan_collection(&coll, dir, lim).await.unwrap_or_default()
        }
    });

    let docs = docs.read();
    let docs = docs.as_deref().unwrap_or(&[]);

    let collection_for_insert = collection.clone();

    rsx! {
        div { class: "card",
            div { class: "form-row",
                label { "Direction:" }
                select {
                    class: "input",
                    onchange: move |e| {
                        direction.set(if e.value() == "forward" {
                            ScanDirection::Forward
                        } else {
                            ScanDirection::Backward
                        });
                    },
                    option { value: "backward", selected: dir == ScanDirection::Backward, "Newest First" }
                    option { value: "forward", selected: dir == ScanDirection::Forward, "Oldest First" }
                }
                label { "Limit:" }
                input {
                    class: "input input-small",
                    r#type: "number",
                    value: "{lim}",
                    oninput: move |e| {
                        if let Ok(n) = e.value().parse::<usize>() {
                            limit.set(n.max(1).min(1000));
                        }
                    },
                }
                button {
                    class: "btn",
                    onclick: move |_| insert_mode.set(!insert_mode.cloned()),
                    if *insert_mode.read() { "Cancel" } else { "Insert Document" }
                }
            }
        }

        if *insert_mode.read() {
            div { class: "card",
                h4 { "Insert Document" }
                textarea {
                    class: "json-editor",
                    rows: "8",
                    value: "{insert_json.read()}",
                    oninput: move |e| insert_json.set(e.value()),
                }
                {
                    let is_valid = serde_json::from_str::<serde_json::Value>(&insert_json.read()).is_ok();
                    rsx! {
                        div { class: "form-row",
                            span {
                                class: if is_valid { "json-valid" } else { "json-invalid" },
                                if is_valid { "Valid JSON" } else { "Invalid JSON" }
                            }
                            button {
                                class: "btn btn-action",
                                disabled: !is_valid,
                                onclick: {
                                    let coll = collection_for_insert.clone();
                                    move |_| {
                                        let json_str = insert_json.read().clone();
                                        let coll = coll.clone();
                                        spawn(async move {
                                            let engine = state.engine.read().clone();
                                            if let Some(engine) = engine {
                                                match serde_json::from_str::<serde_json::Value>(&json_str) {
                                                    Ok(body) => {
                                                        match engine.l6.insert_doc(&coll, body).await {
                                                            Ok((doc_id, exdb::TransactionResult::Success { .. })) => {
                                                                let id_hex: String = doc_id.0.iter().map(|b| format!("{b:02x}")).collect();
                                                                state.notify_mutation();
                                                                state.last_result.set(Some(OperationResult::Success(
                                                                    format!("Inserted {id_hex}"),
                                                                )));
                                                                insert_mode.set(false);
                                                                insert_json.set("{\n  \n}".to_string());
                                                            }
                                                            Ok((_, exdb::TransactionResult::Conflict { error, .. })) => {
                                                                state.last_result.set(Some(OperationResult::Error(
                                                                    format!("Conflict: {error}"),
                                                                )));
                                                            }
                                                            Err(e) => {
                                                                state.last_result.set(Some(OperationResult::Error(format!("{e}"))));
                                                            }
                                                            _ => {}
                                                        }
                                                    }
                                                    Err(e) => {
                                                        state.last_result.set(Some(OperationResult::Error(format!("JSON parse: {e}"))));
                                                    }
                                                }
                                            }
                                        });
                                    }
                                },
                                "Insert"
                            }
                        }
                    }
                }
            }
        }

        div { class: "card",
            h4 { "{docs.len()} documents" }
            if docs.is_empty() {
                div { class: "empty-state",
                    div { "No documents in this collection" }
                }
            } else {
                for (i, doc) in docs.iter().enumerate() {
                    {
                        let is_selected = *selected_doc.read() == Some(i);
                        let doc_id = doc.get("_meta")
                            .and_then(|m| m.get("id"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        let preview = doc_preview(doc);
                        let json_full = serde_json::to_string_pretty(doc).unwrap_or_default();
                        let collection_for_delete = collection.clone();
                        let doc_for_delete = doc.clone();
                        rsx! {
                            div {
                                class: if is_selected { "doc-card selected" } else { "doc-card" },
                                onclick: move |_| {
                                    if is_selected {
                                        selected_doc.set(None);
                                    } else {
                                        selected_doc.set(Some(i));
                                    }
                                },
                                div { class: "doc-card-header",
                                    span { class: "mono doc-id", "{doc_id}" }
                                    if !is_selected {
                                        span { class: "doc-preview", "{preview}" }
                                    }
                                }
                                if is_selected {
                                    pre { class: "doc-json", "{json_full}" }
                                    div { class: "doc-actions",
                                        button {
                                            class: "btn btn-small btn-danger",
                                            onclick: {
                                                let coll = collection_for_delete.clone();
                                                let doc = doc_for_delete.clone();
                                                move |evt| {
                                                    evt.stop_propagation();
                                                    let coll = coll.clone();
                                                    let doc = doc.clone();
                                                    spawn(async move {
                                                        let engine = state.engine.read().clone();
                                                        if let Some(engine) = engine {
                                                            if let Some(id) = extract_doc_id(&doc) {
                                                                match engine.l6.delete_doc(&coll, &id).await {
                                                                    Ok(exdb::TransactionResult::Success { .. }) => {
                                                                        state.notify_mutation();
                                                                        selected_doc.set(None);
                                                                        state.last_result.set(Some(OperationResult::Success("Document deleted".into())));
                                                                    }
                                                                    Err(e) => {
                                                                        state.last_result.set(Some(OperationResult::Error(format!("{e}"))));
                                                                    }
                                                                    _ => {}
                                                                }
                                                            }
                                                        }
                                                    });
                                                }
                                            },
                                            "Delete"
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
}

fn doc_preview(doc: &serde_json::Value) -> String {
    let s = serde_json::to_string(doc).unwrap_or_default();
    if s.len() > 120 {
        format!("{}...", &s[..120])
    } else {
        s
    }
}

fn extract_doc_id(doc: &serde_json::Value) -> Option<exdb::DocId> {
    let hex = doc.get("_meta")?.get("id")?.as_str()?;
    let bytes: Vec<u8> = (0..hex.len())
        .step_by(2)
        .filter_map(|i| u8::from_str_radix(hex.get(i..i + 2)?, 16).ok())
        .collect();
    if bytes.len() == 16 {
        let mut arr = [0u8; 16];
        arr.copy_from_slice(&bytes);
        Some(exdb::DocId(arr))
    } else {
        None
    }
}
