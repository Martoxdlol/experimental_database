use dioxus::prelude::*;

use crate::state::{AppState, OperationResult};

#[component]
pub fn IndexManager(collection: String) -> Element {
    let mut state = use_context::<AppState>();
    let _rev = *state.revision.read();

    let engine = state.engine.read().clone();
    let coll = collection.clone();

    let indexes = use_resource(move || {
        let engine = engine.clone();
        let coll = coll.clone();
        async move {
            let Some(engine) = engine else { return Vec::new() };
            engine.l6.list_indexes(&coll).await.unwrap_or_default()
        }
    });

    let mut new_name = use_signal(|| String::new());
    let mut new_fields = use_signal(|| String::new());

    let indexes = indexes.read();
    let indexes = indexes.as_deref().unwrap_or(&[]);
    let collection_for_create = collection.clone();

    rsx! {
        div { class: "card",
            h4 { "Create Index" }
            div { class: "form-row",
                input {
                    class: "input",
                    r#type: "text",
                    placeholder: "Index name...",
                    value: "{new_name.read()}",
                    oninput: move |e| new_name.set(e.value()),
                }
                input {
                    class: "input",
                    r#type: "text",
                    placeholder: "Field paths (comma-separated, e.g. name, address.city)...",
                    value: "{new_fields.read()}",
                    oninput: move |e| new_fields.set(e.value()),
                }
                button {
                    class: "btn btn-action",
                    disabled: new_name.read().is_empty() || new_fields.read().is_empty(),
                    onclick: {
                        let coll = collection_for_create.clone();
                        move |_| {
                            let name = new_name.read().clone();
                            let fields_str = new_fields.read().clone();
                            let coll = coll.clone();
                            let fields: Vec<exdb::FieldPath> = fields_str
                                .split(',')
                                .map(|s| {
                                    let segments: Vec<String> = s.trim().split('.').map(|p| p.to_string()).collect();
                                    exdb::FieldPath::new(segments)
                                })
                                .collect();
                            spawn(async move {
                                let engine = state.engine.read().clone();
                                if let Some(engine) = engine {
                                    match engine.l6.create_index(&coll, &name, fields).await {
                                        Ok(exdb::TransactionResult::Success { .. }) => {
                                            state.notify_mutation();
                                            state.last_result.set(Some(OperationResult::Success(
                                                format!("Index '{name}' created"),
                                            )));
                                            new_name.set(String::new());
                                            new_fields.set(String::new());
                                        }
                                        Ok(exdb::TransactionResult::Conflict { error, .. }) => {
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
                            });
                        }
                    },
                    "Create Index"
                }
            }
        }

        div { class: "card",
            h4 { "Indexes ({indexes.len()})" }
            if indexes.is_empty() {
                div { class: "empty-state", "No indexes" }
            } else {
                table { class: "data-table",
                    thead {
                        tr {
                            th { "Name" }
                            th { "Fields" }
                            th { "State" }
                            th { "Root Page" }
                            th { "" }
                        }
                    }
                    tbody {
                        for idx in indexes {
                            {
                                let state_class = match idx.state {
                                    exdb::IndexState::Ready => "index-badge ready",
                                    exdb::IndexState::Building => "index-badge building",
                                    exdb::IndexState::Dropping => "index-badge dropping",
                                };
                                let state_label = format!("{:?}", idx.state);
                                let fields_str = idx.field_paths
                                    .iter()
                                    .map(|fp| fp.segments().join("."))
                                    .collect::<Vec<_>>()
                                    .join(", ");
                                let idx_name = idx.name.clone();
                                let collection_for_drop = collection.clone();
                                let is_system = idx_name.starts_with('_');
                                rsx! {
                                    tr {
                                        td { class: "mono", "{idx_name}" }
                                        td { class: "mono", "{fields_str}" }
                                        td { span { class: "{state_class}", "{state_label}" } }
                                        td { "{idx.root_page}" }
                                        td {
                                            if !is_system {
                                                button {
                                                    class: "btn btn-small btn-danger",
                                                    onclick: {
                                                        let coll = collection_for_drop.clone();
                                                        let name = idx_name.clone();
                                                        move |_| {
                                                            let coll = coll.clone();
                                                            let name = name.clone();
                                                            spawn(async move {
                                                                let engine = state.engine.read().clone();
                                                                if let Some(engine) = engine {
                                                                    match engine.l6.drop_index(&coll, &name).await {
                                                                        Ok(exdb::TransactionResult::Success { .. }) => {
                                                                            state.notify_mutation();
                                                                            state.last_result.set(Some(OperationResult::Success(
                                                                                format!("Index '{name}' dropped"),
                                                                            )));
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
                                                    "Drop"
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
