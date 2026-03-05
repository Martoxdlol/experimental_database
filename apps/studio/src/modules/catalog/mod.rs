use dioxus::prelude::*;

use crate::state::AppState;

#[component]
pub fn CatalogModule() -> Element {
    let state = use_context::<AppState>();
    let db = state.db.read().clone();
    let _write_enabled = *state.write_enabled.read();

    let Some(db) = db else {
        return rsx! { div { class: "empty-state", "No database open" } };
    };

    let collections = use_resource(move || {
        let db = db.clone();
        async move {
            tokio::task::spawn_blocking(move || db.list_collections())
                .await
                .ok()
                .and_then(|r| r.ok())
        }
    });

    let mut selected_collection: Signal<Option<u64>> = use_signal(|| None);

    rsx! {
        div { class: "main-content",
            h2 { style: "margin-bottom: 16px;", "Catalog Browser" }

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
                                    }
                                }
                                tbody {
                                    for col in cols {
                                        {
                                            let col_id = col.id;
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
                                            }
                                        }
                                        tbody {
                                            for idx in &col.indexes {
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
