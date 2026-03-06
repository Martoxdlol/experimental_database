use dioxus::prelude::*;

use crate::state::AppState;

type IndexSelection = (u64, String, u32, Vec<Vec<String>>);

#[component]
pub fn SecondaryIndexModule() -> Element {
    let state = use_context::<AppState>();
    let db = state.db.read().clone();

    let Some(db) = db else {
        return rsx! { div { class: "empty-state", "No database open" } };
    };

    let mut selected_collection: Signal<Option<(u64, String, u32)>> = use_signal(|| None);
    let mut selected_index: Signal<Option<IndexSelection>> = use_signal(|| None);
    let mut scan_value: Signal<String> = use_signal(String::new);
    let mut read_ts_input: Signal<String> = use_signal(|| "max".to_string());
    let mut results: Signal<Option<Vec<crate::engine::SecondaryHit>>> = use_signal(|| None);

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
        div { class: "main-content",
            h2 { style: "margin-bottom: 16px;", "Secondary Index Scanner" }

            // Selector card
            div { class: "card",
                div { class: "card-header", "Select Index" }
                div { class: "card-body",
                    div { style: "display: flex; gap: 12px; align-items: flex-end; flex-wrap: wrap;",
                        // Collection selector
                        div {
                            label { style: "color: var(--text-secondary); font-size: 11px; display: block;",
                                "Collection:"
                            }
                            select {
                                class: "select",
                                onchange: move |e| {
                                    let val = e.value();
                                    if let Some(Some(cols)) = collections.read().as_ref()
                                        && let Ok(id) = val.parse::<u64>()
                                        && let Some(col) = cols.iter().find(|c| c.id == id)
                                    {
                                        selected_collection.set(Some((col.id, col.name.clone(), col.data_root_page)));
                                        selected_index.set(None);
                                        results.set(None);
                                    }
                                },
                                option { value: "", "Choose..." }
                                if let Some(Some(cols)) = collections.read().as_ref() {
                                    for col in cols {
                                        option { value: "{col.id}", "{col.name}" }
                                    }
                                }
                            }
                        }

                        // Index selector
                        if let Some((col_id, _, _)) = selected_collection.read().clone() {
                            div {
                                label { style: "color: var(--text-secondary); font-size: 11px; display: block;",
                                    "Index:"
                                }
                                select {
                                    class: "select",
                                    onchange: move |e| {
                                        let val = e.value();
                                        if let Some(Some(cols)) = collections.read().as_ref()
                                            && let Some(col) = cols.iter().find(|c| c.id == col_id)
                                            && let Ok(id) = val.parse::<u64>()
                                            && let Some(idx) = col.indexes.iter().find(|i| i.id == id)
                                        {
                                            selected_index.set(Some((
                                                idx.id,
                                                idx.name.clone(),
                                                idx.root_page,
                                                idx.field_paths.clone(),
                                            )));
                                            results.set(None);
                                        }
                                    },
                                    option { value: "", "Choose..." }
                                    if let Some(Some(cols)) = collections.read().as_ref() {
                                        if let Some(col) = cols.iter().find(|c| c.id == col_id) {
                                            for idx in &col.indexes {
                                                {
                                                    let fields: Vec<String> = idx.field_paths.iter()
                                                        .map(|fp| fp.join(".")).collect();
                                                    rsx! {
                                                        option { value: "{idx.id}",
                                                            "{idx.name} ({fields.join(\", \")})"
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

            // Query card
            if let Some((_, idx_name, sec_root, field_paths)) = selected_index.read().clone() {
                if let Some((_, _, primary_root)) = selected_collection.read().clone() {
                    div { class: "card",
                        div { class: "card-header", "Query: {idx_name}" }
                        div { class: "card-body",
                            div { style: "margin-bottom: 8px;",
                                {
                                    let fields: Vec<String> = field_paths.iter()
                                        .map(|fp| fp.join(".")).collect();
                                    rsx! {
                                        div { style: "color: var(--text-secondary); font-size: 11px; margin-bottom: 4px;",
                                            "Fields: {fields.join(\", \")}"
                                        }
                                    }
                                }
                            }
                            div { style: "display: flex; gap: 8px; align-items: flex-end; flex-wrap: wrap;",
                                div { style: "flex: 1;",
                                    label { style: "color: var(--text-secondary); font-size: 11px; display: block;",
                                        "Scan value (JSON array of scalars):"
                                    }
                                    input {
                                        class: "edit-input",
                                        style: "width: 100%;",
                                        placeholder: r#"e.g. ["Alice"] or [42]"#,
                                        value: "{scan_value.read()}",
                                        oninput: move |e| scan_value.set(e.value()),
                                    }
                                }
                                div {
                                    label { style: "color: var(--text-secondary); font-size: 11px; display: block;",
                                        "read_ts:"
                                    }
                                    input {
                                        class: "edit-input",
                                        style: "width: 80px;",
                                        value: "{read_ts_input.read()}",
                                        oninput: move |e| read_ts_input.set(e.value()),
                                    }
                                }
                                button {
                                    class: "btn btn-action",
                                    onclick: move |_| {
                                        let db = state.db.read().clone();
                                        let sv = scan_value.read().clone();
                                        let ts_str = read_ts_input.read().clone();
                                        let read_ts = if ts_str == "max" || ts_str.is_empty() {
                                            u64::MAX
                                        } else {
                                            ts_str.parse().unwrap_or(u64::MAX)
                                        };
                                        if let Some(db) = db {
                                            match db.secondary_scan(sec_root, primary_root, &sv, read_ts, 200) {
                                                Ok(hits) => results.set(Some(hits)),
                                                Err(e) => {
                                                    let mut s = state;
                                                    s.last_result.set(Some(crate::state::OperationResult::Error(
                                                        e.to_string()
                                                    )));
                                                    results.set(None);
                                                }
                                            }
                                        }
                                    },
                                    "Scan"
                                }
                            }
                        }
                    }
                }
            }

            // Results
            if let Some(hits) = results.read().as_ref() {
                div { class: "card",
                    div { class: "card-header", "{hits.len()} results" }
                    div { class: "card-body",
                        if hits.is_empty() {
                            div { style: "color: var(--text-secondary);", "No matching documents" }
                        } else {
                            table { class: "slot-table",
                                thead {
                                    tr {
                                        th { "#" }
                                        th { "DocId" }
                                        th { "Version TS" }
                                    }
                                }
                                tbody {
                                    for (i, hit) in hits.iter().enumerate() {
                                        tr {
                                            td { "{i}" }
                                            td {
                                                span { style: "font-family: monospace; font-size: 11px;",
                                                    "{short_id(&hit.doc_id_hex)}"
                                                }
                                            }
                                            td { "{hit.version_ts}" }
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

fn short_id(hex: &str) -> String {
    if hex.len() > 12 {
        format!("{}...{}", &hex[..8], &hex[hex.len() - 4..])
    } else {
        hex.to_string()
    }
}
