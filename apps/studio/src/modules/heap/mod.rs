use dioxus::prelude::*;

use crate::components::HexDump;
use crate::state::{AppState, OperationResult};
use exdb_storage::page::PageType;

#[component]
pub fn HeapModule() -> Element {
    let state = use_context::<AppState>();
    let db = state.db.read().clone();
    let write_enabled = *state.write_enabled.read();

    let Some(db) = db else {
        return rsx! { div { class: "empty-state", "No database open" } };
    };

    // Find all heap pages
    let db_for_scan = db.clone();
    let heap_pages = use_resource(move || {
        let db = db_for_scan.clone();
        async move {
            tokio::task::spawn_blocking(move || {
                let pages = db.scan_page_types()?;
                let heap_pages: Vec<_> = pages
                    .into_iter()
                    .filter(|(_, pt, _)| *pt == Some(PageType::Heap))
                    .map(|(id, _, _)| {
                        let info = db.read_page(id)?;
                        Ok((id, info.num_slots, info.free_space))
                    })
                    .collect::<std::io::Result<Vec<_>>>()?;
                Ok::<_, std::io::Error>(heap_pages)
            })
            .await
            .ok()
            .and_then(|r| r.ok())
        }
    });

    rsx! {
        div { class: "main-content",
            h2 { style: "margin-bottom: 16px;", "Heap Inspector" }

            // Heap pages table
            div { class: "card",
                div { class: "card-header",
                    "Heap Pages"
                }
                div { class: "card-body",
                    if let Some(Some(pages)) = heap_pages.read().as_ref() {
                        table { class: "slot-table",
                            thead {
                                tr {
                                    th { "Page" }
                                    th { "Slots" }
                                    th { "Free Space" }
                                    th { "Usage" }
                                }
                            }
                            tbody {
                                for (page_id, slots, free) in pages {
                                    {
                                        let pid = *page_id;
                                        let page_size = db.page_size;
                                        let used_pct = if page_size > 0 {
                                            ((page_size - *free) as f64 / page_size as f64) * 100.0
                                        } else { 0.0 };
                                        rsx! {
                                            tr {
                                                td { span { class: "page-link", "#{pid}" } }
                                                td { "{slots}" }
                                                td { "{free}" }
                                                td {
                                                    div { style: "display: flex; align-items: center; gap: 8px;",
                                                        div {
                                                            style: "width: 120px; height: 12px; background: var(--bg-primary); border-radius: 3px; overflow: hidden;",
                                                            div {
                                                                style: "height: 100%; width: {used_pct:.0}%; background: #bb9af7; border-radius: 3px;",
                                                            }
                                                        }
                                                        span { style: "font-size: 10px; color: var(--text-secondary);",
                                                            "{used_pct:.0}%"
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
                        div { style: "color: var(--text-secondary);", "No heap pages found" }
                    }
                }
            }

            // Store blob form (write mode)
            if write_enabled {
                StoreBlobForm {}
            }

            // Load blob form
            div { class: "card",
                div { class: "card-header", "Load Blob" }
                div { class: "card-body",
                    LoadBlobForm {}
                }
            }

            // Free blob form (write mode)
            if write_enabled {
                FreeBlobForm {}
            }
        }
    }
}

#[component]
fn StoreBlobForm() -> Element {
    let mut state = use_context::<AppState>();
    let mut mode: Signal<String> = use_signal(|| "text".to_string());
    let mut text_input: Signal<String> = use_signal(String::new);
    let mut hex_input: Signal<String> = use_signal(String::new);

    rsx! {
        div { class: "card",
            div { class: "card-header", "Store Blob" }
            div { class: "card-body",
                div { style: "display: flex; gap: 8px; margin-bottom: 8px;",
                    label { style: "font-size: 11px; color: var(--text-secondary);",
                        input {
                            r#type: "radio",
                            name: "store_mode",
                            checked: *mode.read() == "text",
                            onchange: move |_| mode.set("text".to_string()),
                        }
                        " Text / JSON"
                    }
                    label { style: "font-size: 11px; color: var(--text-secondary);",
                        input {
                            r#type: "radio",
                            name: "store_mode",
                            checked: *mode.read() == "hex",
                            onchange: move |_| mode.set("hex".to_string()),
                        }
                        " Hex"
                    }
                }
                if *mode.read() == "text" {
                    textarea {
                        class: "edit-input",
                        style: "width: 100%; height: 80px; font-family: monospace; resize: vertical;",
                        placeholder: "Text or JSON data to store...",
                        value: "{text_input.read()}",
                        oninput: move |e| text_input.set(e.value()),
                    }
                } else {
                    input {
                        class: "edit-input",
                        style: "width: 100%;",
                        placeholder: "Hex bytes: e.g. 48 65 6C 6C 6F",
                        value: "{hex_input.read()}",
                        oninput: move |e| hex_input.set(e.value()),
                    }
                }
                button {
                    class: "btn btn-action",
                    style: "margin-top: 8px;",
                    onclick: move |_| {
                        let data = if *mode.read() == "text" {
                            Some(text_input.read().as_bytes().to_vec())
                        } else {
                            parse_hex_bytes(&hex_input.read())
                        };
                        if let Some(data) = data {
                            if data.is_empty() {
                                state.last_result.set(Some(OperationResult::Error("Empty data".into())));
                                return;
                            }
                            let db = state.db.read().clone();
                            if let Some(db) = db {
                                match db.heap_store(&data) {
                                    Ok((page_id, slot_id)) => {
                                        state.last_result.set(Some(OperationResult::Success(
                                            format!("Stored {} bytes at page={page_id}, slot={slot_id}", data.len())
                                        )));
                                        text_input.set(String::new());
                                        hex_input.set(String::new());
                                    }
                                    Err(e) => state.last_result.set(Some(OperationResult::Error(e.to_string()))),
                                }
                            }
                        } else {
                            state.last_result.set(Some(OperationResult::Error("Invalid hex input".into())));
                        }
                    },
                    "Store"
                }
            }
        }
    }
}

#[component]
fn LoadBlobForm() -> Element {
    let mut state = use_context::<AppState>();
    let mut page_input: Signal<String> = use_signal(String::new);
    let mut slot_input: Signal<String> = use_signal(String::new);
    let mut blob_data: Signal<Option<Vec<u8>>> = use_signal(|| None);

    rsx! {
        div { style: "display: flex; gap: 8px; align-items: flex-end; margin-bottom: 12px;",
            div {
                label { style: "color: var(--text-secondary); font-size: 11px; display: block;", "Page ID:" }
                input {
                    class: "edit-input",
                    style: "width: 80px;",
                    value: "{page_input.read()}",
                    oninput: move |e| page_input.set(e.value()),
                }
            }
            div {
                label { style: "color: var(--text-secondary); font-size: 11px; display: block;", "Slot ID:" }
                input {
                    class: "edit-input",
                    style: "width: 80px;",
                    value: "{slot_input.read()}",
                    oninput: move |e| slot_input.set(e.value()),
                }
            }
            button {
                class: "btn",
                onclick: move |_| {
                    let page = page_input.read().parse::<u32>().unwrap_or(0);
                    let slot = slot_input.read().parse::<u16>().unwrap_or(0);
                    let db = state.db.read().clone();
                    if let Some(db) = db {
                        match db.heap_load(page, slot) {
                            Ok(data) => blob_data.set(Some(data)),
                            Err(e) => {
                                state.last_result.set(Some(OperationResult::Error(e.to_string())));
                                blob_data.set(None);
                            }
                        }
                    }
                },
                "Load"
            }
        }

        if let Some(data) = blob_data.read().as_ref() {
            div { style: "margin-bottom: 8px; color: var(--text-secondary); font-size: 11px;",
                "{data.len()} bytes"
            }
            // Try to display as UTF-8 / JSON
            if let Ok(text) = std::str::from_utf8(data) {
                div {
                    style: "padding: 8px; background: var(--bg-primary); border: 1px solid var(--border); border-radius: 4px; font-size: 12px; white-space: pre-wrap; max-height: 300px; overflow: auto;",
                    "{text}"
                }
            }
            HexDump { data: data.clone() }
        }
    }
}

#[component]
fn FreeBlobForm() -> Element {
    let mut state = use_context::<AppState>();
    let mut page_input: Signal<String> = use_signal(String::new);
    let mut slot_input: Signal<String> = use_signal(String::new);

    rsx! {
        div { class: "card",
            div { class: "card-header", "Free Blob" }
            div { class: "card-body",
                div { style: "display: flex; gap: 8px; align-items: flex-end;",
                    div {
                        label { style: "color: var(--text-secondary); font-size: 11px; display: block;", "Page ID:" }
                        input {
                            class: "edit-input",
                            style: "width: 80px;",
                            value: "{page_input.read()}",
                            oninput: move |e| page_input.set(e.value()),
                        }
                    }
                    div {
                        label { style: "color: var(--text-secondary); font-size: 11px; display: block;", "Slot ID:" }
                        input {
                            class: "edit-input",
                            style: "width: 80px;",
                            value: "{slot_input.read()}",
                            oninput: move |e| slot_input.set(e.value()),
                        }
                    }
                    button {
                        class: "btn btn-danger",
                        onclick: move |_| {
                            let page_val = page_input.read().clone();
                            let slot_val = slot_input.read().clone();
                            if let (Ok(page), Ok(slot)) = (page_val.parse::<u32>(), slot_val.parse::<u16>()) {
                                let db = state.db.read().clone();
                                if let Some(db) = db {
                                    match db.heap_free(page, slot) {
                                        Ok(()) => {
                                            state.last_result.set(Some(OperationResult::Success(
                                                format!("Freed blob at page={page}, slot={slot}")
                                            )));
                                            page_input.set(String::new());
                                            slot_input.set(String::new());
                                        }
                                        Err(e) => state.last_result.set(Some(OperationResult::Error(e.to_string()))),
                                    }
                                }
                            } else {
                                state.last_result.set(Some(OperationResult::Error("Invalid page/slot ID".into())));
                            }
                        },
                        "Free"
                    }
                }
            }
        }
    }
}

fn parse_hex_bytes(s: &str) -> Option<Vec<u8>> {
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
