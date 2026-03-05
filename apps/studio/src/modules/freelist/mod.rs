use dioxus::prelude::*;

use crate::state::{AppState, OperationResult};

#[component]
pub fn FreeListModule() -> Element {
    let mut state = use_context::<AppState>();
    let db = state.db.read().clone();
    let write_enabled = *state.write_enabled.read();

    let Some(db) = db else {
        return rsx! { div { class: "empty-state", "No database open" } };
    };

    let free_list = use_resource(move || {
        let db = db.clone();
        async move {
            tokio::task::spawn_blocking(move || db.walk_free_list())
                .await
                .ok()
                .and_then(|r| r.ok())
        }
    });

    rsx! {
        div { class: "main-content",
            h2 { style: "margin-bottom: 16px;", "Free List" }

            if let Some(Some(info)) = free_list.read().as_ref() {
                div { class: "card",
                    div { class: "card-header", "Chain" }
                    div { class: "card-body",
                        table { class: "kv-table",
                            tr {
                                td { "Head" }
                                td {
                                    if info.head != 0 {
                                        span { class: "page-link", "Page #{info.head}" }
                                    } else {
                                        span { class: "kv-value", "(empty)" }
                                    }
                                }
                            }
                            tr {
                                td { "Length" }
                                td { span { class: "kv-value", "{info.chain.len()} pages" } }
                            }
                        }

                        if !info.chain.is_empty() {
                            div { style: "margin-top: 12px; display: flex; flex-wrap: wrap; align-items: center; gap: 4px;",
                                for (i, page_id) in info.chain.iter().enumerate() {
                                    span { class: "page-link", "Page #{page_id}" }
                                    if i < info.chain.len() - 1 {
                                        span { style: "color: var(--text-secondary);", " \u{2192} " }
                                    }
                                }
                                span { style: "color: var(--text-secondary);", " \u{2192} \u{2205}" }
                            }
                        }
                    }
                }

                // Write actions
                if write_enabled {
                    div { class: "card",
                        div { class: "card-header", "Actions" }
                        div { class: "card-body",
                            div { style: "display: flex; gap: 8px;",
                                button {
                                    class: "btn btn-action",
                                    onclick: move |_| {
                                        let db = state.db.read().clone();
                                        if let Some(db) = db {
                                            match db.free_list_allocate() {
                                                Ok(page_id) => {
                                                    state.last_result.set(Some(OperationResult::Success(
                                                        format!("Allocated page #{page_id}")
                                                    )));
                                                }
                                                Err(e) => {
                                                    state.last_result.set(Some(OperationResult::Error(e.to_string())));
                                                }
                                            }
                                        }
                                    },
                                    "Allocate Page"
                                }

                                DeallocateForm {}
                            }
                        }
                    }
                }
            } else {
                div { "Loading free list..." }
            }
        }
    }
}

#[component]
fn DeallocateForm() -> Element {
    let mut state = use_context::<AppState>();
    let mut page_input: Signal<String> = use_signal(|| String::new());

    rsx! {
        div { style: "display: flex; gap: 8px; align-items: center;",
            input {
                class: "edit-input",
                style: "width: 80px;",
                placeholder: "Page ID",
                value: "{page_input.read()}",
                oninput: move |e| page_input.set(e.value()),
            }
            button {
                class: "btn btn-danger",
                onclick: move |_| {
                    let val = page_input.read().clone();
                    if let Ok(page_id) = val.parse::<u32>() {
                        let db = state.db.read().clone();
                        if let Some(db) = db {
                            match db.free_list_deallocate(page_id) {
                                Ok(()) => {
                                    state.last_result.set(Some(OperationResult::Success(
                                        format!("Deallocated page #{page_id}")
                                    )));
                                    page_input.set(String::new());
                                }
                                Err(e) => {
                                    state.last_result.set(Some(OperationResult::Error(e.to_string())));
                                }
                            }
                        }
                    }
                },
                "Deallocate"
            }
        }
    }
}
