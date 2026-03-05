use dioxus::prelude::*;

use crate::state::{AppState, OperationResult};

#[component]
pub fn BTreeModule() -> Element {
    let mut state = use_context::<AppState>();
    let db = state.db.read().clone();
    let write_enabled = *state.write_enabled.read();

    let Some(db) = db else {
        return rsx! { div { class: "empty-state", "No database open" } };
    };

    let mut root_input: Signal<String> = use_signal(String::new);
    let mut selected_root: Signal<Option<u32>> = use_signal(|| None);

    // Build tree selector from catalog
    let db_for_catalog = db.clone();
    let collections = use_resource(move || {
        let db = db_for_catalog.clone();
        async move {
            tokio::task::spawn_blocking(move || db.list_collections())
                .await
                .ok()
                .and_then(|r| r.ok())
        }
    });

    rsx! {
        div { class: "main-content",
            h2 { style: "margin-bottom: 16px;", "B-Tree Explorer" }

            // Tree selector
            div { class: "card",
                div { class: "card-header", "Select B-Tree" }
                div { class: "card-body",
                    div { style: "display: flex; align-items: center; gap: 12px; flex-wrap: wrap;",
                        // Known trees from catalog
                        if let Some(Some(cols)) = collections.read().as_ref() {
                            select {
                                class: "select",
                                onchange: move |e| {
                                    if let Ok(root) = e.value().parse::<u32>() {
                                        selected_root.set(Some(root));
                                    }
                                },
                                option { value: "", "Choose a tree..." }
                                {
                                    let fh = db.read_file_header();
                                    let cat_root = fh.catalog_root_page.get();
                                    let name_root = fh.catalog_name_root_page.get();
                                    rsx! {
                                        if cat_root != 0 {
                                            option { value: "{cat_root}", "Catalog (by-id) - Page #{cat_root}" }
                                        }
                                        if name_root != 0 {
                                            option { value: "{name_root}", "Catalog (by-name) - Page #{name_root}" }
                                        }
                                        for col in cols {
                                            option {
                                                value: "{col.data_root_page}",
                                                "{col.name} - Page #{col.data_root_page}"
                                            }
                                            for idx in &col.indexes {
                                                option {
                                                    value: "{idx.root_page}",
                                                    "  {col.name}.{idx.name} - Page #{idx.root_page}"
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        span { style: "color: var(--text-secondary);", "or" }

                        // Manual root page input
                        input {
                            class: "edit-input",
                            placeholder: "Root page ID",
                            value: "{root_input.read()}",
                            oninput: move |e| root_input.set(e.value()),
                            onkeydown: move |e| {
                                if e.key() == Key::Enter
                                    && let Ok(root) = root_input.read().parse::<u32>() {
                                        selected_root.set(Some(root));
                                    }
                            },
                        }
                        button {
                            class: "btn",
                            onclick: move |_| {
                                if let Ok(root) = root_input.read().parse::<u32>() {
                                    selected_root.set(Some(root));
                                }
                            },
                            "Open"
                        }

                        if write_enabled {
                            button {
                                class: "btn btn-action",
                                onclick: move |_| {
                                    let db = state.db.read().clone();
                                    if let Some(db) = db {
                                        match db.create_btree() {
                                            Ok(root) => {
                                                selected_root.set(Some(root));
                                                state.last_result.set(Some(OperationResult::Success(
                                                    format!("Created new B-tree with root page #{root}")
                                                )));
                                            }
                                            Err(e) => {
                                                state.last_result.set(Some(OperationResult::Error(e.to_string())));
                                            }
                                        }
                                    }
                                },
                                "+ New B-Tree"
                            }
                        }
                    }
                }
            }

            // Tree view
            if let Some(root) = *selected_root.read() {
                BTreeView { root_page: root, write_enabled }
            }
        }
    }
}

#[component]
fn BTreeView(root_page: u32, write_enabled: bool) -> Element {
    let state = use_context::<AppState>();
    let db = state.db.read().clone();

    let Some(db) = db else {
        return rsx! { "No database" };
    };

    // Load node
    let node = use_resource(move || {
        let db = db.clone();
        async move {
            tokio::task::spawn_blocking(move || db.read_btree_node(root_page))
                .await
                .ok()
                .and_then(|r| r.ok())
        }
    });

    match node.read().as_ref() {
        Some(Some(info)) => {
            let node_type = if info.is_leaf { "Leaf" } else { "Internal" };

            rsx! {
                div { class: "card",
                    div { class: "card-header",
                        "[{node_type} #{info.page_id}] ({info.num_keys} keys)"
                    }
                    div { class: "card-body",
                        if info.is_leaf {
                            // Show key-value entries
                            table { class: "slot-table",
                                thead {
                                    tr {
                                        th { "#" }
                                        th { "Key" }
                                        th { "Value" }
                                    }
                                }
                                tbody {
                                    for (i, entry) in info.entries.iter().enumerate() {
                                        tr {
                                            td { "{i}" }
                                            td { "{hex_short(&entry.key)}" }
                                            td { "{hex_short(&entry.value)}" }
                                        }
                                    }
                                }
                            }
                        } else {
                            // Show tree structure with children
                            for (i, child) in info.children.iter().enumerate() {
                                div { style: "margin: 4px 0;",
                                    if i > 0 {
                                        if let Some(entry) = info.entries.get(i - 1) {
                                            div {
                                                style: "padding: 2px 8px; color: var(--text-secondary); font-size: 11px;",
                                                "key: {hex_short(&entry.key)}"
                                            }
                                        }
                                    }
                                    div {
                                        style: "padding: 2px 8px;",
                                        span { style: "color: var(--text-secondary);", "child{i} -> " }
                                        span { class: "page-link", "Page #{child}" }
                                    }
                                }
                            }
                        }
                    }
                }

                // Insert key-value form
                if write_enabled && info.is_leaf {
                    InsertKvForm { root_page }
                }
            }
        }
        _ => rsx! { div { "Loading B-tree node..." } },
    }
}

#[component]
fn InsertKvForm(root_page: u32) -> Element {
    let mut state = use_context::<AppState>();
    let mut key_hex: Signal<String> = use_signal(String::new);
    let mut value_hex: Signal<String> = use_signal(String::new);

    rsx! {
        div { class: "card",
            div { class: "card-header", "Insert Key-Value" }
            div { class: "card-body",
                div { style: "display: flex; flex-direction: column; gap: 8px;",
                    div {
                        label { style: "color: var(--text-secondary); font-size: 11px;", "Key (hex):" }
                        input {
                            class: "edit-input",
                            style: "width: 100%;",
                            placeholder: "e.g. 0001020304",
                            value: "{key_hex.read()}",
                            oninput: move |e| key_hex.set(e.value()),
                        }
                    }
                    div {
                        label { style: "color: var(--text-secondary); font-size: 11px;", "Value (hex):" }
                        input {
                            class: "edit-input",
                            style: "width: 100%;",
                            placeholder: "e.g. 48656C6C6F",
                            value: "{value_hex.read()}",
                            oninput: move |e| value_hex.set(e.value()),
                        }
                    }
                    button {
                        class: "btn btn-action",
                        onclick: move |_| {
                            let key = parse_hex(&key_hex.read());
                            let value = parse_hex(&value_hex.read());
                            if let (Some(k), Some(v)) = (key, value) {
                                let db = state.db.read().clone();
                                if let Some(db) = db {
                                    match db.btree_insert(root_page, &k, &v) {
                                        Ok(()) => {
                                            state.last_result.set(Some(OperationResult::Success("Key inserted".into())));
                                            key_hex.set(String::new());
                                            value_hex.set(String::new());
                                        }
                                        Err(e) => {
                                            state.last_result.set(Some(OperationResult::Error(e.to_string())));
                                        }
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
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}
