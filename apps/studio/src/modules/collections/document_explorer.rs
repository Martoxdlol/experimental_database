use dioxus::prelude::*;

use exdb::{DocId, ScanDirection};

use crate::state::{AppState, OperationResult};

/// Prisma/Beekeeper-style full-screen data table.
#[component]
pub fn DocumentExplorer(collection: String) -> Element {
    let mut state = use_context::<AppState>();

    // ─── Pagination & sorting state ───
    let mut page = use_signal(|| 0usize);
    let mut page_size = use_signal(|| 50usize);
    let mut direction = use_signal(|| ScanDirection::Backward);

    // ─── Editing state ───
    let mut editing: Signal<Option<EditingCell>> = use_signal(|| None);
    let mut edit_buffer = use_signal(|| String::new());

    // ─── Row detail / insert state ───
    let mut expanded_row: Signal<Option<usize>> = use_signal(|| None);
    let mut insert_mode = use_signal(|| false);
    let mut insert_json = use_signal(|| "{\n  \n}".to_string());

    // ─── Fetch documents ───
    // Read signals reactively inside the resource so it re-fetches on mutation.
    let engine_sig = state.engine;
    let rev_sig = state.revision;
    let coll_for_fetch = collection.clone();

    let docs_resource = use_resource(move || {
        let engine = engine_sig.read().clone();
        let coll = coll_for_fetch.clone();
        let dir = *direction.read();
        let ps = *page_size.read();
        let pg = *page.read();
        let _rev = *rev_sig.read(); // reactive dependency — triggers re-fetch on mutation
        let total_fetch = (pg + 1) * ps + 1;
        async move {
            let Some(engine) = engine else {
                return Vec::new();
            };
            engine
                .l6
                .scan_with_ids(&coll, dir, total_fetch)
                .await
                .unwrap_or_default()
        }
    });

    // Clone out of the resource to get owned data
    let all_rows: Vec<(DocId, serde_json::Value)> = docs_resource
        .read()
        .as_ref()
        .map(|v| v.clone())
        .unwrap_or_default();

    let ps = *page_size.read();
    let pg = *page.read();
    let dir = *direction.read();
    let start = pg * ps;
    let page_rows: Vec<(DocId, serde_json::Value)> = if start < all_rows.len() {
        let end = (start + ps).min(all_rows.len());
        all_rows[start..end].to_vec()
    } else {
        Vec::new()
    };
    let has_prev = pg > 0;
    let has_next = all_rows.len() > (pg + 1) * ps;
    let showing_from = if page_rows.is_empty() { 0 } else { start + 1 };
    let showing_to = start + page_rows.len();
    let total_loaded = all_rows.len().min((pg + 1) * ps);

    // Extract just the docs for column detection
    let page_docs: Vec<&serde_json::Value> = page_rows.iter().map(|(_, doc)| doc).collect();
    let columns = detect_columns(&page_docs);
    let col_count = columns.len() + 2; // +row_num +actions

    let collection_for_insert = collection.clone();

    rsx! {
        div { class: "grid-panel",
            // ─── Toolbar strip ───
            div { class: "grid-toolbar",
                div { class: "grid-toolbar-left",
                    button {
                        class: if *insert_mode.read() { "grid-btn active" } else { "grid-btn accent" },
                        onclick: move |_| {
                            insert_mode.set(!insert_mode.cloned());
                            if !*insert_mode.read() {
                                insert_json.set("{\n  \n}".to_string());
                            }
                        },
                        if *insert_mode.read() { "Cancel" } else { "+ Add Row" }
                    }
                    div { class: "grid-toolbar-sep" }
                    select {
                        class: "grid-select",
                        onchange: move |e| {
                            direction.set(if e.value() == "forward" {
                                ScanDirection::Forward
                            } else {
                                ScanDirection::Backward
                            });
                            page.set(0);
                        },
                        option { value: "backward", selected: dir == ScanDirection::Backward, "Newest first" }
                        option { value: "forward", selected: dir == ScanDirection::Forward, "Oldest first" }
                    }
                }
                div { class: "grid-toolbar-right",
                    span { class: "grid-status",
                        if page_docs.is_empty() {
                            "0 rows"
                        } else {
                            "{showing_from}\u{2013}{showing_to} of {total_loaded}+"
                        }
                    }
                    button {
                        class: "grid-btn",
                        disabled: !has_prev,
                        onclick: move |_| page.set(pg.saturating_sub(1)),
                        "\u{25C0}"
                    }
                    button {
                        class: "grid-btn",
                        disabled: !has_next,
                        onclick: move |_| page.set(pg + 1),
                        "\u{25B6}"
                    }
                    select {
                        class: "grid-select",
                        value: "{ps}",
                        onchange: move |e| {
                            if let Ok(n) = e.value().parse::<usize>() {
                                page_size.set(n.max(10).min(500));
                                page.set(0);
                            }
                        },
                        option { value: "25", "25" }
                        option { value: "50", "50" }
                        option { value: "100", "100" }
                        option { value: "200", "200" }
                    }
                }
            }

            // ─── Insert row panel ───
            if *insert_mode.read() {
                div { class: "grid-insert-bar",
                    textarea {
                        class: "grid-insert-editor",
                        rows: "4",
                        value: "{insert_json.read()}",
                        oninput: move |e| insert_json.set(e.value()),
                        placeholder: "{{ \"name\": \"Alice\", \"age\": 30 }}",
                    }
                    {
                        let is_valid = serde_json::from_str::<serde_json::Value>(&insert_json.read()).is_ok();
                        rsx! {
                            div { class: "grid-insert-actions",
                                span {
                                    class: if is_valid { "json-valid" } else { "json-invalid" },
                                    if is_valid { "Valid JSON" } else { "Invalid JSON" }
                                }
                                button {
                                    class: "grid-btn accent",
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
                                                                Ok((_doc_id, exdb::TransactionResult::Success { .. })) => {
                                                                    state.notify_mutation();
                                                                    state.last_result.set(Some(OperationResult::Success("Row inserted".into())));
                                                                    insert_mode.set(false);
                                                                    insert_json.set("{\n  \n}".to_string());
                                                                }
                                                                Ok((_, exdb::TransactionResult::Conflict { error, .. })) => {
                                                                    state.last_result.set(Some(OperationResult::Error(format!("Conflict: {error}"))));
                                                                }
                                                                Err(e) => {
                                                                    state.last_result.set(Some(OperationResult::Error(format!("{e}"))));
                                                                }
                                                                _ => {}
                                                            }
                                                        }
                                                        Err(e) => {
                                                            state.last_result.set(Some(OperationResult::Error(format!("JSON: {e}"))));
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

            // ─── Full-bleed data grid ───
            div { class: "grid-scroll",
                if page_rows.is_empty() && !*insert_mode.read() {
                    div { class: "grid-empty",
                        div { style: "font-size: 32px; margin-bottom: 8px;", "\u{1F4CB}" }
                        div { "No documents" }
                        div { style: "color: var(--text-secondary); font-size: 12px; margin-top: 4px;",
                            "Click \"+ Add Row\" to insert data"
                        }
                    }
                } else if !page_rows.is_empty() {
                    table { class: "studio-grid",
                        thead {
                            tr {
                                th { class: "grid-row-num", "#" }
                                for col in &columns {
                                    th { class: "grid-col-header", "{col}" }
                                }
                                th { class: "grid-actions-col", "" }
                            }
                        }
                        tbody {
                            for (row_idx, (doc_id, doc)) in page_rows.into_iter().enumerate() {
                                {
                                    let is_expanded = *expanded_row.read() == Some(row_idx);
                                    let row_num = start + row_idx + 1;
                                    let collection_for_row = collection.clone();

                                    let cells: Vec<CellData> = columns.iter().map(|col| {
                                        let cell_val = doc.get(col.as_str());
                                        CellData {
                                            col: col.clone(),
                                            display: format_cell(cell_val),
                                            raw: format_cell_raw(cell_val),
                                            is_null: cell_val.is_none() || cell_val == Some(&serde_json::Value::Null),
                                        }
                                    }).collect();

                                    let doc_json = serde_json::to_string_pretty(&doc).unwrap_or_default();
                                    let delete_id = doc_id;

                                    rsx! {
                                        tr {
                                            class: if is_expanded { "grid-row expanded" } else { "grid-row" },
                                            // Row number
                                            td {
                                                class: "grid-row-num",
                                                onclick: move |_| {
                                                    expanded_row.set(if is_expanded { None } else { Some(row_idx) });
                                                },
                                                "{row_num}"
                                            }
                                            // Cells
                                            for cell in cells {
                                                {
                                                    let is_editing = editing.read().as_ref().map_or(false, |e| e.row == row_idx && e.col == cell.col);

                                                    if is_editing {
                                                        let coll = collection_for_row.clone();
                                                        let save_id = doc_id;
                                                        let col_save = cell.col.clone();
                                                        rsx! {
                                                            td { class: "grid-cell editing",
                                                                input {
                                                                    class: "grid-cell-input",
                                                                    r#type: "text",
                                                                    value: "{edit_buffer.read()}",
                                                                    autofocus: true,
                                                                    oninput: move |e| edit_buffer.set(e.value()),
                                                                    onkeydown: {
                                                                        let coll_k = coll.clone();
                                                                        let id_k = save_id;
                                                                        let cn_k = col_save.clone();
                                                                        move |e: KeyboardEvent| {
                                                                            if e.key() == Key::Enter {
                                                                                let coll = coll_k.clone();
                                                                                let cn = cn_k.clone();
                                                                                let val = edit_buffer.read().clone();
                                                                                spawn(async move {
                                                                                    save_cell_edit(&mut state, &coll, &id_k, &cn, &val).await;
                                                                                    editing.set(None);
                                                                                });
                                                                            } else if e.key() == Key::Escape {
                                                                                editing.set(None);
                                                                            }
                                                                        }
                                                                    },
                                                                    // Save on blur (click away) — same as Enter
                                                                    onfocusout: {
                                                                        let coll_b = coll.clone();
                                                                        let id_b = save_id;
                                                                        let cn_b = col_save.clone();
                                                                        move |_| {
                                                                            let coll = coll_b.clone();
                                                                            let cn = cn_b.clone();
                                                                            let val = edit_buffer.read().clone();
                                                                            spawn(async move {
                                                                                save_cell_edit(&mut state, &coll, &id_b, &cn, &val).await;
                                                                                editing.set(None);
                                                                            });
                                                                        }
                                                                    },
                                                                }
                                                            }
                                                        }
                                                    } else {
                                                        let col_for_edit = cell.col.clone();
                                                        let raw_for_edit = cell.raw.clone();
                                                        rsx! {
                                                            td {
                                                                class: if cell.is_null { "grid-cell null-cell" } else { "grid-cell" },
                                                                ondoubleclick: move |_| {
                                                                    if !col_for_edit.starts_with('_') {
                                                                        edit_buffer.set(raw_for_edit.clone());
                                                                        editing.set(Some(EditingCell {
                                                                            row: row_idx,
                                                                            col: col_for_edit.clone(),
                                                                        }));
                                                                    }
                                                                },
                                                                span { class: "cell-text", "{cell.display}" }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            // Delete
                                            td { class: "grid-actions-cell",
                                                button {
                                                    class: "btn-icon btn-icon-danger",
                                                    title: "Delete row",
                                                    onclick: {
                                                        let coll = collection_for_row.clone();
                                                        move |_| {
                                                            let coll = coll.clone();
                                                            spawn(async move {
                                                                let engine = state.engine.read().clone();
                                                                if let Some(engine) = engine {
                                                                    match engine.l6.delete_doc(&coll, &delete_id).await {
                                                                        Ok(exdb::TransactionResult::Success { .. }) => {
                                                                            state.notify_mutation();
                                                                            expanded_row.set(None);
                                                                            state.last_result.set(Some(OperationResult::Success("Row deleted".into())));
                                                                        }
                                                                        Err(e) => {
                                                                            state.last_result.set(Some(OperationResult::Error(format!("{e}"))));
                                                                        }
                                                                        _ => {}
                                                                    }
                                                                }
                                                            });
                                                        }
                                                    },
                                                    "\u{2715}"
                                                }
                                            }
                                        }
                                        // Expanded JSON detail
                                        if is_expanded {
                                            tr { class: "grid-detail-row",
                                                td { colspan: "{col_count}",
                                                    div { class: "row-detail",
                                                        pre { class: "doc-json", "{doc_json}" }
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
        }
    }
}

// ─── Types ───

#[derive(Clone, Debug, PartialEq)]
struct EditingCell {
    row: usize,
    col: String,
}

struct CellData {
    col: String,
    display: String,
    raw: String,
    is_null: bool,
}

// ─── Cell editing ───

async fn save_cell_edit(
    state: &mut AppState,
    collection: &str,
    doc_id: &DocId,
    field: &str,
    new_value_str: &str,
) {
    let engine = state.engine.read().clone();
    let Some(engine) = engine else { return };

    let new_value = parse_cell_value(new_value_str);
    let mut map = serde_json::Map::new();
    map.insert(field.to_string(), new_value);
    let patch = serde_json::Value::Object(map);

    match engine.l6.patch_doc(collection, &doc_id, patch).await {
        Ok(exdb::TransactionResult::Success { .. }) => {
            state.notify_mutation();
            state.last_result.set(Some(OperationResult::Success(format!("Updated {field}"))));
        }
        Ok(exdb::TransactionResult::Conflict { error, .. }) => {
            state.last_result.set(Some(OperationResult::Error(format!("Conflict: {error}"))));
        }
        Err(e) => {
            state.last_result.set(Some(OperationResult::Error(format!("{e}"))));
        }
        _ => {}
    }
}

fn parse_cell_value(s: &str) -> serde_json::Value {
    let trimmed = s.trim();
    if trimmed.is_empty() || trimmed == "null" {
        return serde_json::Value::Null;
    }
    if trimmed == "true" { return serde_json::Value::Bool(true); }
    if trimmed == "false" { return serde_json::Value::Bool(false); }
    if let Ok(n) = trimmed.parse::<i64>() { return serde_json::json!(n); }
    if let Ok(n) = trimmed.parse::<f64>() { return serde_json::json!(n); }
    if (trimmed.starts_with('{') || trimmed.starts_with('['))
        && let Ok(v) = serde_json::from_str::<serde_json::Value>(trimmed)
    {
        return v;
    }
    serde_json::Value::String(s.to_string())
}

// ─── Column detection ───

fn detect_columns(docs: &[&serde_json::Value]) -> Vec<String> {
    let mut cols = Vec::new();
    for doc in docs {
        if let serde_json::Value::Object(map) = doc {
            for key in map.keys() {
                if !cols.contains(key) {
                    cols.push(key.clone());
                }
            }
        }
    }
    cols.sort_by(|a, b| {
        let rank = |s: &str| -> u8 {
            if s == "_meta" { 2 } else if s.starts_with('_') { 1 } else { 0 }
        };
        rank(a).cmp(&rank(b)).then(a.cmp(b))
    });
    cols
}

// ─── Cell formatting ───

fn format_cell(v: Option<&serde_json::Value>) -> String {
    match v {
        None | Some(serde_json::Value::Null) => "NULL".to_string(),
        Some(serde_json::Value::String(s)) => {
            if s.len() > 80 { format!("{}...", &s[..80]) } else { s.clone() }
        }
        Some(serde_json::Value::Number(n)) => n.to_string(),
        Some(serde_json::Value::Bool(b)) => b.to_string(),
        Some(serde_json::Value::Object(_)) => "{...}".to_string(),
        Some(serde_json::Value::Array(a)) => format!("[{} items]", a.len()),
    }
}

fn format_cell_raw(v: Option<&serde_json::Value>) -> String {
    match v {
        None | Some(serde_json::Value::Null) => String::new(),
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Number(n)) => n.to_string(),
        Some(serde_json::Value::Bool(b)) => b.to_string(),
        Some(v) => serde_json::to_string(v).unwrap_or_default(),
    }
}

