mod document_explorer;
mod index_manager;

use dioxus::prelude::*;

use crate::state::{AppState, CollectionView, OperationResult};

pub use document_explorer::DocumentExplorer;
pub use index_manager::IndexManager;

#[component]
pub fn CollectionsModule() -> Element {
    let mut state = use_context::<AppState>();
    let _rev = *state.revision.read();
    let selected = state.selected_collection.read().clone();

    if let Some(ref name) = selected {
        return rsx! { CollectionDetail { name: name.clone() } };
    }

    // Collection browser
    let engine = state.engine.read().clone();
    let collections = engine.as_ref().map(|e| e.l6.list_collections()).unwrap_or_default();

    let mut new_name = use_signal(|| String::new());

    rsx! {
        div { class: "main-content",
            div { class: "card",
                h3 { "Collections" }
                div { class: "form-row",
                    input {
                        class: "input",
                        r#type: "text",
                        placeholder: "New collection name...",
                        value: "{new_name.read()}",
                        oninput: move |e| new_name.set(e.value()),
                    }
                    button {
                        class: "btn btn-action",
                        disabled: new_name.read().is_empty(),
                        onclick: move |_| {
                            let name = new_name.read().clone();
                            spawn(async move {
                                let engine = state.engine.read().clone();
                                if let Some(engine) = engine {
                                    match engine.l6.create_collection(&name).await {
                                        Ok(exdb::TransactionResult::Success { .. }) => {
                                            state.notify_mutation();
                                            state.last_result.set(Some(OperationResult::Success(
                                                format!("Collection '{name}' created"),
                                            )));
                                            new_name.set(String::new());
                                        }
                                        Ok(exdb::TransactionResult::Conflict { error, .. }) => {
                                            state.last_result.set(Some(OperationResult::Error(
                                                format!("Conflict: {error}"),
                                            )));
                                        }
                                        Err(e) => {
                                            state.last_result.set(Some(OperationResult::Error(
                                                format!("{e}"),
                                            )));
                                        }
                                        _ => {}
                                    }
                                }
                            });
                        },
                        "Create"
                    }
                }
            }

            if collections.is_empty() {
                div { class: "card",
                    div { class: "empty-state",
                        div { class: "empty-state-icon", "\u{1F4C1}" }
                        div { "No collections" }
                        div { style: "color: var(--text-secondary); font-size: 12px; margin-top: 4px;",
                            "Create a collection above to get started"
                        }
                    }
                }
            } else {
                div { class: "card",
                    table { class: "data-table",
                        thead {
                            tr {
                                th { "Name" }
                                th { "Documents" }
                                th { "" }
                            }
                        }
                        tbody {
                            for col in collections {
                                {
                                    let name = col.name.clone();
                                    let name2 = name.clone();
                                    let name3 = name.clone();
                                    rsx! {
                                        tr {
                                            class: "clickable-row",
                                            onclick: move |_| {
                                                state.selected_collection.set(Some(name.clone()));
                                                state.collection_view.set(CollectionView::Documents);
                                                state.breadcrumb.set(vec![
                                                    "Database".to_string(),
                                                    "Collections".to_string(),
                                                    name.clone(),
                                                ]);
                                            },
                                            td { class: "mono", "{name2}" }
                                            td { "{col.doc_count}" }
                                            td {
                                                button {
                                                    class: "btn btn-small btn-danger",
                                                    onclick: move |evt| {
                                                        evt.stop_propagation();
                                                        let cname = name3.clone();
                                                        spawn(async move {
                                                            let engine = state.engine.read().clone();
                                                            if let Some(engine) = engine {
                                                                match engine.l6.drop_collection(&cname).await {
                                                                    Ok(exdb::TransactionResult::Success { .. }) => {
                                                                        state.notify_mutation();
                                                                        state.last_result.set(Some(OperationResult::Success(
                                                                            format!("Collection '{cname}' dropped"),
                                                                        )));
                                                                    }
                                                                    Err(e) => {
                                                                        state.last_result.set(Some(OperationResult::Error(format!("{e}"))));
                                                                    }
                                                                    _ => {}
                                                                }
                                                            }
                                                        });
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

#[component]
fn CollectionDetail(name: String) -> Element {
    let mut state = use_context::<AppState>();
    let view = *state.collection_view.read();

    // Full-bleed layout — no .main-content wrapper, fills the entire content area
    rsx! {
        div { class: "collection-detail",
            // Compact header bar
            div { class: "collection-bar",
                button {
                    class: "grid-btn",
                    onclick: move |_| {
                        state.selected_collection.set(None);
                        state.breadcrumb.set(vec![
                            "Database".to_string(),
                            "Collections".to_string(),
                        ]);
                    },
                    "\u{2190}"
                }
                span { class: "collection-bar-name", "{name}" }
                div { class: "collection-bar-tabs",
                    div {
                        class: if view == CollectionView::Documents { "bar-tab active" } else { "bar-tab" },
                        onclick: move |_| state.collection_view.set(CollectionView::Documents),
                        "Data"
                    }
                    div {
                        class: if view == CollectionView::Indexes { "bar-tab active" } else { "bar-tab" },
                        onclick: move |_| state.collection_view.set(CollectionView::Indexes),
                        "Indexes"
                    }
                }
            }
            // Content fills remaining space
            match view {
                CollectionView::Documents => rsx! { DocumentExplorer { collection: name.clone() } },
                CollectionView::Indexes => rsx! {
                    div { class: "main-content",
                        IndexManager { collection: name.clone() }
                    }
                },
            }
        }
    }
}
