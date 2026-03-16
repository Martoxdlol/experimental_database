use dioxus::prelude::*;

use crate::state::{AppState, OperationResult};

#[component]
pub fn CatalogModule() -> Element {
    let mut state = use_context::<AppState>();
    let db = state.db.read().clone();
    let write_enabled = *state.write_enabled.read();

    let Some(db) = db else {
        return rsx! { div { class: "empty-state", "No database open" } };
    };

    let collections = use_resource(move || {
        let _rev = *state.revision.read();
        let db = db.clone();
        async move {
            db.list_collections().await.ok()
        }
    });

    let mut selected_collection: Signal<Option<u64>> = use_signal(|| None);

    rsx! {
        div { class: "main-content",
            h2 { style: "margin-bottom: 16px;", "Catalog Browser" }

            // Create collection form
            if write_enabled {
                CreateCollectionForm {}
            }

            // Collections table
            div { class: "card",
                div { class: "card-header",
                    "Collections"
                }
                div { class: "card-body",
                    if let Some(Some(cols)) = collections.read().as_ref() {
                        if cols.is_empty() {
                            div { style: "color: var(--text-secondary);", "No collections found" }
                        } else {
                            table { class: "slot-table",
                                thead {
                                    tr {
                                        th { "ID" }
                                        th { "Name" }
                                        th { "Data Root" }
                                        th { "Doc Count" }
                                        th { "Indexes" }
                                        if write_enabled {
                                            th { "Actions" }
                                        }
                                    }
                                }
                                tbody {
                                    for col in cols {
                                        {
                                            let col_id = col.id;
                                            let col_name = col.name.clone();
                                            let sel = *selected_collection.read() == Some(col_id);
                                            rsx! {
                                                tr {
                                                    class: if sel { "selected" } else { "" },
                                                    onclick: move |_| selected_collection.set(Some(col_id)),
                                                    td { "{col.id}" }
                                                    td { "{col.name}" }
                                                    td { span { class: "page-link", "Page #{col.data_root_page}" } }
                                                    td { "{col.doc_count}" }
                                                    td { "{col.indexes.len()}" }
                                                    if write_enabled {
                                                        td {
                                                            button {
                                                                class: "edit-btn",
                                                                title: "Drop collection",
                                                                onclick: move |e| {
                                                                    e.stop_propagation();
                                                                    let db = state.db.read().clone();
                                                                    let col_name = col_name.clone();
                                                                    spawn(async move {
                                                                        if let Some(db) = db {
                                                                            match db.catalog_drop_collection(col_id, &col_name).await {
                                                                                Ok(()) => {
                                                                                    state.last_result.set(Some(OperationResult::Success(
                                                                                        format!("Dropped collection '{col_name}'")
                                                                                    )));
                                                                                    state.notify_mutation();
                                                                                }
                                                                                Err(e) => state.last_result.set(Some(OperationResult::Error(e.to_string()))),
                                                                            }
                                                                        }
                                                                    });
                                                                },
                                                                "\u{2717}"
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
                    } else {
                        div { "Loading catalog..." }
                    }
                }
            }

            // Index detail for selected collection
            if let Some(sel_id) = *selected_collection.read() {
                if let Some(Some(cols)) = collections.read().as_ref() {
                    if let Some(col) = cols.iter().find(|c| c.id == sel_id) {
                        div { class: "card",
                            div { class: "card-header",
                                "Indexes for \"{col.name}\""
                            }
                            div { class: "card-body",
                                if col.indexes.is_empty() {
                                    div { style: "color: var(--text-secondary);", "No indexes" }
                                } else {
                                    table { class: "slot-table",
                                        thead {
                                            tr {
                                                th { "ID" }
                                                th { "Name" }
                                                th { "Root Page" }
                                                th { "Status" }
                                                th { "Fields" }
                                                if write_enabled {
                                                    th { "Actions" }
                                                }
                                            }
                                        }
                                        tbody {
                                            for idx in &col.indexes {
                                                {
                                                    let idx_id = idx.id;
                                                    rsx! {
                                                        tr {
                                                            td { "{idx.id}" }
                                                            td { "{idx.name}" }
                                                            td { span { class: "page-link", "Page #{idx.root_page}" } }
                                                            td {
                                                                {
                                                                    let badge_class = match idx.status.as_str() {
                                                                        "Ready" => "status-badge ok",
                                                                        "Building" => "status-badge",
                                                                        _ => "status-badge bad",
                                                                    };
                                                                    rsx! {
                                                                        span { class: "{badge_class}", "{idx.status}" }
                                                                    }
                                                                }
                                                            }
                                                            td {
                                                                {
                                                                    let fields: Vec<String> = idx.field_paths.iter()
                                                                        .map(|fp| fp.join("."))
                                                                        .collect();
                                                                    rsx! { "{fields.join(\", \")}" }
                                                                }
                                                            }
                                                            if write_enabled {
                                                                td {
                                                                    button {
                                                                        class: "edit-btn",
                                                                        title: "Drop index",
                                                                        onclick: move |e| {
                                                                            e.stop_propagation();
                                                                            let db = state.db.read().clone();
                                                                            spawn(async move {
                                                                                if let Some(db) = db {
                                                                                    match db.catalog_drop_index(idx_id).await {
                                                                                        Ok(()) => {
                                                                                            state.last_result.set(Some(OperationResult::Success(
                                                                                                format!("Dropped index #{idx_id}")
                                                                                            )));
                                                                                            state.notify_mutation();
                                                                                        }
                                                                                        Err(e) => state.last_result.set(Some(OperationResult::Error(e.to_string()))),
                                                                                    }
                                                                                }
                                                                            });
                                                                        },
                                                                        "\u{2717}"
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

                        // Create index form
                        if write_enabled {
                            CreateIndexForm { collection_id: sel_id }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn CreateCollectionForm() -> Element {
    let mut state = use_context::<AppState>();
    let mut name_input: Signal<String> = use_signal(String::new);

    rsx! {
        div { class: "card",
            div { class: "card-header", "Create Collection" }
            div { class: "card-body",
                div { style: "display: flex; gap: 8px; align-items: flex-end;",
                    div { style: "flex: 1;",
                        label { style: "color: var(--text-secondary); font-size: 11px; display: block;", "Name:" }
                        input {
                            class: "edit-input",
                            style: "width: 100%;",
                            placeholder: "collection_name",
                            value: "{name_input.read()}",
                            oninput: move |e| name_input.set(e.value()),
                        }
                    }
                    button {
                        class: "btn btn-action",
                        onclick: move |_| {
                            let name = name_input.read().clone();
                            if name.is_empty() {
                                state.last_result.set(Some(OperationResult::Error("Name required".into())));
                                return;
                            }
                            let db = state.db.read().clone();
                            spawn(async move {
                                if let Some(db) = db {
                                    match db.catalog_create_collection(&name).await {
                                        Ok(info) => {
                                            state.last_result.set(Some(OperationResult::Success(
                                                format!("Created collection '{}' (id={}, root={})", info.name, info.id, info.data_root_page)
                                            )));
                                            state.notify_mutation();
                                            name_input.set(String::new());
                                        }
                                        Err(e) => state.last_result.set(Some(OperationResult::Error(e.to_string()))),
                                    }
                                }
                            });
                        },
                        "Create"
                    }
                }
            }
        }
    }
}

#[component]
fn CreateIndexForm(collection_id: u64) -> Element {
    let mut state = use_context::<AppState>();
    let mut name_input: Signal<String> = use_signal(String::new);
    let mut fields_input: Signal<String> = use_signal(String::new);

    rsx! {
        div { class: "card",
            div { class: "card-header", "Create Index" }
            div { class: "card-body",
                div { style: "display: flex; gap: 8px; align-items: flex-end; flex-wrap: wrap;",
                    div {
                        label { style: "color: var(--text-secondary); font-size: 11px; display: block;", "Index Name:" }
                        input {
                            class: "edit-input",
                            style: "width: 180px;",
                            placeholder: "idx_field",
                            value: "{name_input.read()}",
                            oninput: move |e| name_input.set(e.value()),
                        }
                    }
                    div { style: "flex: 1;",
                        label { style: "color: var(--text-secondary); font-size: 11px; display: block;", "Fields (comma-separated):" }
                        input {
                            class: "edit-input",
                            style: "width: 100%;",
                            placeholder: "e.g. name, address.city",
                            value: "{fields_input.read()}",
                            oninput: move |e| fields_input.set(e.value()),
                        }
                    }
                    button {
                        class: "btn btn-action",
                        onclick: move |_| {
                            let name = name_input.read().clone();
                            let fields_str = fields_input.read().clone();
                            if name.is_empty() || fields_str.is_empty() {
                                state.last_result.set(Some(OperationResult::Error("Name and fields required".into())));
                                return;
                            }
                            let field_paths: Vec<Vec<String>> = fields_str
                                .split(',')
                                .map(|f| f.trim().split('.').map(|s| s.to_string()).collect())
                                .collect();
                            let db = state.db.read().clone();
                            spawn(async move {
                                if let Some(db) = db {
                                    match db.catalog_create_index(collection_id, &name, field_paths).await {
                                        Ok(info) => {
                                            state.last_result.set(Some(OperationResult::Success(
                                                format!("Created index '{}' (id={}, root={})", info.name, info.id, info.root_page)
                                            )));
                                            state.notify_mutation();
                                            name_input.set(String::new());
                                            fields_input.set(String::new());
                                        }
                                        Err(e) => state.last_result.set(Some(OperationResult::Error(e.to_string()))),
                                    }
                                }
                            });
                        },
                        "Create Index"
                    }
                }
            }
        }
    }
}
