use dioxus::prelude::*;

use crate::components::HexDump;
use crate::state::{AppState, OperationResult};
use exdb_storage::page::PageType;

#[component]
pub fn PagesModule() -> Element {
    let mut state = use_context::<AppState>();
    let db = state.db.read().clone();
    let write_enabled = *state.write_enabled.read();

    let Some(db) = db else {
        return rsx! { div { class: "empty-state", "No database open" } };
    };

    let mut selected_page: Signal<Option<u32>> = use_signal(|| None);
    let mut selected_slot: Signal<Option<u16>> = use_signal(|| None);
    let mut type_filter: Signal<String> = use_signal(|| "All".to_string());
    let mut search_filter: Signal<String> = use_signal(String::new);

    // Load page list
    let page_list = use_resource(move || {
        let _rev = *state.revision.read();
        let db = db.clone();
        async move {
            db.scan_page_types().await.ok()
        }
    });

    // Load selected page detail
    let _page_detail = use_memo(move || {

        *selected_page.read()
    });

    rsx! {
        div { class: "master-detail",
            // Page list (left panel)
            div { class: "master-panel",
                div { style: "padding: 8px;",
                    input {
                        class: "filter-input",
                        placeholder: "Filter by page ID...",
                        value: "{search_filter.read()}",
                        oninput: move |e| search_filter.set(e.value()),
                    }
                    select {
                        class: "select",
                        style: "width: 100%; margin-bottom: 8px;",
                        value: "{type_filter.read()}",
                        onchange: move |e| type_filter.set(e.value()),
                        option { value: "All", "All Types" }
                        option { value: "BTreeLeaf", "BTreeLeaf" }
                        option { value: "BTreeInternal", "BTreeInternal" }
                        option { value: "Heap", "Heap" }
                        option { value: "Overflow", "Overflow" }
                        option { value: "Free", "Free" }
                        option { value: "FileHeader", "FileHeader" }
                    }
                }

                if let Some(Some(pages)) = page_list.read().as_ref() {
                    {
                        let filter = type_filter.read().clone();
                        let search = search_filter.read().clone();
                        let sel = *selected_page.read();
                        let filtered: Vec<_> = pages.iter().filter(|(id, pt, _raw)| {
                            // Type filter
                            if filter != "All" {
                                let type_name = pt.map(page_type_name).unwrap_or("Unknown");
                                if type_name != filter {
                                    return false;
                                }
                            }
                            // Search filter
                            if !search.is_empty() {
                                if let Ok(n) = search.parse::<u32>() {
                                    return *id == n;
                                }
                                return false;
                            }
                            true
                        }).collect();

                        rsx! {
                            div { style: "overflow-y: auto;",
                                for (id, pt, _raw) in &filtered {
                                    {
                                        let page_id = *id;
                                        let is_sel = sel == Some(page_id);
                                        let type_name = pt.map(page_type_name).unwrap_or("Unknown");
                                        let badge_class = pt.map(page_type_badge_class).unwrap_or("");
                                        rsx! {
                                            div {
                                                class: if is_sel { "list-item selected" } else { "list-item" },
                                                onclick: move |_| {
                                                    selected_page.set(Some(page_id));
                                                    selected_slot.set(None);
                                                    state.breadcrumb.set(vec![
                                                        "Database".to_string(),
                                                        "Pages".to_string(),
                                                        format!("Page #{page_id}"),
                                                    ]);
                                                },
                                                span { style: "color: var(--text-secondary); min-width: 36px;", "#{page_id}" }
                                                span { class: "page-type-badge {badge_class}", "{type_name}" }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Page detail (right panel)
            div { class: "detail-panel",
                if let Some(page_id) = *selected_page.read() {
                    PageDetail { page_id, write_enabled, selected_slot }
                } else {
                    div { class: "empty-state",
                        div { class: "empty-state-icon", "\u{1F4C4}" }
                        "Select a page to inspect"
                    }
                }
            }
        }
    }
}

#[component]
fn PageDetail(page_id: u32, write_enabled: bool, selected_slot: Signal<Option<u16>>) -> Element {
    let mut state = use_context::<AppState>();
    let db = state.db.read().clone();

    let Some(db) = db else {
        return rsx! { "No database" };
    };

    let page_info = use_resource(move || {
        let _rev = *state.revision.read();
        let db = db.clone();
        async move {
            db.read_page(page_id).await.ok()
        }
    });

    match page_info.read().as_ref() {
        Some(Some(info)) => {
            let type_name = info
                .page_type
                .map(page_type_name)
                .unwrap_or("Unknown");
            let checksum_class = if info.checksum_valid {
                "status-badge ok"
            } else {
                "status-badge bad"
            };
            let checksum_label = if info.checksum_valid { "OK" } else { "BAD" };

            rsx! {
                h3 { style: "margin-bottom: 12px;",
                    "Page #{page_id} \u{2014} {type_name}"
                }

                // Page Header
                div { class: "card",
                    div { class: "card-header", "Header" }
                    div { class: "card-body",
                        table { class: "kv-table",
                            tr {
                                td { "page_id" }
                                td { span { class: "kv-value", "{info.page_id}" } }
                            }
                            tr {
                                td { "page_type" }
                                td { span { class: "kv-value", "{type_name} (0x{info.page_type_raw:02X})" } }
                            }
                            tr {
                                td { "flags" }
                                td { span { class: "kv-value", "0x{info.flags:02X}" } }
                            }
                            tr {
                                td { "num_slots" }
                                td { span { class: "kv-value", "{info.num_slots}" } }
                            }
                            tr {
                                td { "free_space_start" }
                                td { span { class: "kv-value", "{info.free_space_start}" } }
                            }
                            tr {
                                td { "free_space_end" }
                                td { span { class: "kv-value", "{info.free_space_end}" } }
                            }
                            tr {
                                td { "prev_or_ptr" }
                                td {
                                    if info.prev_or_ptr != 0 {
                                        span {
                                            class: "page-link",
                                            "{info.prev_or_ptr}"
                                        }
                                    } else {
                                        span { class: "kv-value", "0 (none)" }
                                    }
                                }
                            }
                            tr {
                                td { "checksum" }
                                td {
                                    span { class: "kv-value", "0x{info.checksum:08X}" }
                                    span { class: "{checksum_class}", " {checksum_label}" }
                                }
                            }
                            tr {
                                td { "lsn" }
                                td { span { class: "kv-value", "{info.lsn}" } }
                            }
                            tr {
                                td { "free_space" }
                                td { span { class: "kv-value", "{info.free_space} bytes" } }
                            }
                        }
                    }
                }

                // Space Map
                {
                    let page_size = info.raw_bytes.len() as u32;
                    let header_size = 32u32;
                    let slot_dir_size = info.num_slots as u32 * 4;
                    let free = info.free_space as u32;
                    let cell_data = page_size.saturating_sub(header_size + slot_dir_size + free);

                    rsx! {
                        div { class: "card",
                            div { class: "card-header", "Space Map" }
                            div { class: "card-body",
                                div { class: "space-bar",
                                    div {
                                        class: "space-bar-segment",
                                        style: "background: var(--page-header); width: {pct(header_size, page_size)}%;",
                                        "HDR"
                                    }
                                    div {
                                        class: "space-bar-segment",
                                        style: "background: var(--page-slots); width: {pct(slot_dir_size, page_size)}%;",
                                        if slot_dir_size > 0 { "SLOTS" }
                                    }
                                    div {
                                        class: "space-bar-segment",
                                        style: "background: var(--page-free); width: {pct(free, page_size)}%;",
                                        "FREE"
                                    }
                                    div {
                                        class: "space-bar-segment",
                                        style: "background: var(--page-cells); width: {pct(cell_data, page_size)}%;",
                                        if cell_data > 0 { "DATA" }
                                    }
                                }
                            }
                        }
                    }
                }

                // Slot Directory
                if info.num_slots > 0 {
                    div { class: "card",
                        div { class: "card-header",
                            "Slot Directory ({info.num_slots} slots)"
                        }
                        div { class: "card-body",
                            table { class: "slot-table",
                                thead {
                                    tr {
                                        th { "#" }
                                        th { "Offset" }
                                        th { "Length" }
                                        th { "Preview" }
                                        if write_enabled {
                                            th { "Actions" }
                                        }
                                    }
                                }
                                tbody {
                                    for slot in &info.slots {
                                        {
                                            let idx = slot.index;
                                            let sel = *selected_slot.read() == Some(idx);
                                            let is_deleted = slot.length == 0;
                                            let preview = if is_deleted {
                                                "(deleted)".to_string()
                                            } else {
                                                hex_preview(&slot.data, 12)
                                            };
                                            let row_class = if sel { "selected" } else { "" };

                                            rsx! {
                                                tr {
                                                    class: "{row_class}",
                                                    onclick: move |_| selected_slot.set(Some(idx)),
                                                    td { "{idx}" }
                                                    td { "{slot.offset}" }
                                                    td {
                                                        if is_deleted {
                                                            span { class: "slot-deleted", "0" }
                                                        } else {
                                                            "{slot.length}"
                                                        }
                                                    }
                                                    td {
                                                        if is_deleted {
                                                            span { class: "slot-deleted", "{preview}" }
                                                        } else {
                                                            "{preview}"
                                                        }
                                                    }
                                                    if write_enabled {
                                                        td {
                                                            if !is_deleted {
                                                                button {
                                                                    class: "edit-btn",
                                                                    title: "Delete slot",
                                                                    onclick: move |e| {
                                                                        e.stop_propagation();
                                                                        let db = state.db.read().clone();
                                                                        spawn(async move {
                                                                            if let Some(db) = db {
                                                                                match db.delete_slot(page_id, idx).await {
                                                                                    Ok(()) => {
                                                                                        state.last_result.set(Some(OperationResult::Success(
                                                                                            format!("Deleted slot #{idx}")
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
                }

                // Write actions
                if write_enabled {
                    div { class: "card",
                        div { class: "card-header", "Actions" }
                        div { class: "card-body",
                            div { style: "display: flex; gap: 8px; flex-wrap: wrap;",
                                button {
                                    class: "btn btn-action",
                                    onclick: move |_| {
                                        let db = state.db.read().clone();
                                        spawn(async move {
                                            if let Some(db) = db {
                                                match db.compact_page(page_id).await {
                                                    Ok(()) => {
                                                        state.last_result.set(Some(OperationResult::Success("Page compacted".into())));
                                                        state.notify_mutation();
                                                    }
                                                    Err(e) => state.last_result.set(Some(OperationResult::Error(e.to_string()))),
                                                }
                                            }
                                        });
                                    },
                                    "Compact"
                                }
                                button {
                                    class: "btn btn-action",
                                    onclick: move |_| {
                                        let db = state.db.read().clone();
                                        spawn(async move {
                                            if let Some(db) = db {
                                                match db.stamp_checksum(page_id).await {
                                                    Ok(()) => {
                                                        state.last_result.set(Some(OperationResult::Success("Checksum stamped".into())));
                                                        state.notify_mutation();
                                                    }
                                                    Err(e) => state.last_result.set(Some(OperationResult::Error(e.to_string()))),
                                                }
                                            }
                                        });
                                    },
                                    "Stamp Checksum"
                                }
                            }
                        }
                    }

                    // Insert Slot Form
                    InsertSlotForm { page_id }

                    // Init Page As Type
                    InitPageForm { page_id }

                    // Update Slot Form
                    if let Some(slot_idx) = *selected_slot.read() {
                        UpdateSlotForm { page_id, slot_index: slot_idx }
                    }
                }

                // Hex Dump
                div { class: "card",
                    div { class: "card-header", "Hex Dump" }
                    div { class: "card-body",
                        HexDump { data: info.raw_bytes.clone() }
                    }
                }
            }
        }
        _ => {
            rsx! { div { "Loading page..." } }
        }
    }
}

#[component]
fn InsertSlotForm(page_id: u32) -> Element {
    let mut state = use_context::<AppState>();
    let mut hex_input: Signal<String> = use_signal(String::new);
    let mut text_input: Signal<String> = use_signal(String::new);
    let mut mode: Signal<String> = use_signal(|| "hex".to_string());

    rsx! {
        div { class: "card",
            div { class: "card-header", "Insert Slot" }
            div { class: "card-body",
                div { style: "display: flex; gap: 8px; margin-bottom: 8px;",
                    label { style: "font-size: 11px; color: var(--text-secondary);",
                        input {
                            r#type: "radio",
                            name: "insert_mode_{page_id}",
                            checked: *mode.read() == "hex",
                            onchange: move |_| mode.set("hex".to_string()),
                        }
                        " Hex"
                    }
                    label { style: "font-size: 11px; color: var(--text-secondary);",
                        input {
                            r#type: "radio",
                            name: "insert_mode_{page_id}",
                            checked: *mode.read() == "text",
                            onchange: move |_| mode.set("text".to_string()),
                        }
                        " UTF-8 Text"
                    }
                }
                if *mode.read() == "hex" {
                    input {
                        class: "edit-input",
                        style: "width: 100%; margin-bottom: 8px;",
                        placeholder: "Hex bytes: e.g. 48 65 6C 6C 6F",
                        value: "{hex_input.read()}",
                        oninput: move |e| hex_input.set(e.value()),
                    }
                } else {
                    input {
                        class: "edit-input",
                        style: "width: 100%; margin-bottom: 8px;",
                        placeholder: "UTF-8 text data",
                        value: "{text_input.read()}",
                        oninput: move |e| text_input.set(e.value()),
                    }
                }
                button {
                    class: "btn btn-action",
                    onclick: move |_| {
                        let current_mode = mode.read().clone();
                        let data = if current_mode == "hex" {
                            let h = hex_input.read().clone();
                            parse_hex_bytes(&h)
                        } else {
                            let t = text_input.read().clone();
                            Some(t.into_bytes())
                        };
                        if let Some(data) = data {
                            let db = state.db.read().clone();
                            spawn(async move {
                                if let Some(db) = db {
                                    match db.insert_slot(page_id, &data).await {
                                        Ok(slot_id) => {
                                            state.last_result.set(Some(OperationResult::Success(
                                                format!("Inserted slot #{slot_id} ({} bytes)", data.len())
                                            )));
                                            state.notify_mutation();
                                            hex_input.set(String::new());
                                            text_input.set(String::new());
                                        }
                                        Err(e) => state.last_result.set(Some(OperationResult::Error(e.to_string()))),
                                    }
                                }
                            });
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

#[component]
fn UpdateSlotForm(page_id: u32, slot_index: u16) -> Element {
    let mut state = use_context::<AppState>();
    let mut hex_input: Signal<String> = use_signal(String::new);

    rsx! {
        div { class: "card",
            div { class: "card-header", "Update Slot #{slot_index}" }
            div { class: "card-body",
                input {
                    class: "edit-input",
                    style: "width: 100%; margin-bottom: 8px;",
                    placeholder: "New hex data for slot",
                    value: "{hex_input.read()}",
                    oninput: move |e| hex_input.set(e.value()),
                }
                button {
                    class: "btn btn-action",
                    onclick: move |_| {
                        let h = hex_input.read().clone();
                        if let Some(data) = parse_hex_bytes(&h) {
                            let db = state.db.read().clone();
                            spawn(async move {
                                if let Some(db) = db {
                                    match db.update_slot(page_id, slot_index, &data).await {
                                        Ok(()) => {
                                            state.last_result.set(Some(OperationResult::Success(
                                                format!("Updated slot #{slot_index}")
                                            )));
                                            state.notify_mutation();
                                            hex_input.set(String::new());
                                        }
                                        Err(e) => state.last_result.set(Some(OperationResult::Error(e.to_string()))),
                                    }
                                }
                            });
                        } else {
                            state.last_result.set(Some(OperationResult::Error("Invalid hex input".into())));
                        }
                    },
                    "Update"
                }
            }
        }
    }
}

#[component]
fn InitPageForm(page_id: u32) -> Element {
    let mut state = use_context::<AppState>();
    let mut page_type_sel: Signal<String> = use_signal(|| "BTreeLeaf".to_string());

    rsx! {
        div { class: "card",
            div { class: "card-header", "Re-Initialize Page" }
            div { class: "card-body",
                div { style: "display: flex; gap: 8px; align-items: flex-end;",
                    div {
                        label { style: "color: var(--text-secondary); font-size: 11px; display: block;", "Page Type:" }
                        select {
                            class: "select",
                            value: "{page_type_sel.read()}",
                            onchange: move |e| page_type_sel.set(e.value()),
                            option { value: "BTreeLeaf", "BTreeLeaf" }
                            option { value: "BTreeInternal", "BTreeInternal" }
                            option { value: "Heap", "Heap" }
                            option { value: "Overflow", "Overflow" }
                            option { value: "Free", "Free" }
                        }
                    }
                    button {
                        class: "btn btn-danger",
                        onclick: move |_| {
                            let pt_name = page_type_sel.read().clone();
                            let pt = match pt_name.as_str() {
                                "BTreeLeaf" => PageType::BTreeLeaf,
                                "BTreeInternal" => PageType::BTreeInternal,
                                "Heap" => PageType::Heap,
                                "Overflow" => PageType::Overflow,
                                "Free" => PageType::Free,
                                _ => return,
                            };
                            let db = state.db.read().clone();
                            spawn(async move {
                                if let Some(db) = db {
                                    match db.init_page(page_id, pt).await {
                                        Ok(()) => {
                                            state.last_result.set(Some(OperationResult::Success(
                                                format!("Page #{page_id} re-initialized as {pt_name}")
                                            )));
                                            state.notify_mutation();
                                        }
                                        Err(e) => state.last_result.set(Some(OperationResult::Error(e.to_string()))),
                                    }
                                }
                            });
                        },
                        "Re-Initialize (destructive)"
                    }
                }
            }
        }
    }
}

fn page_type_name(pt: PageType) -> &'static str {
    match pt {
        PageType::BTreeLeaf => "BTreeLeaf",
        PageType::BTreeInternal => "BTreeInternal",
        PageType::Heap => "Heap",
        PageType::Overflow => "Overflow",
        PageType::Free => "Free",
        PageType::FileHeader => "FileHeader",
        PageType::FileHeaderShadow => "FileHeaderShadow",
    }
}

fn page_type_badge_class(pt: PageType) -> &'static str {
    match pt {
        PageType::BTreeLeaf => "btree-leaf",
        PageType::BTreeInternal => "btree-internal",
        PageType::Heap => "heap",
        PageType::Overflow => "overflow",
        PageType::Free => "free",
        PageType::FileHeader | PageType::FileHeaderShadow => "file-header",
    }
}

fn hex_preview(data: &[u8], max_bytes: usize) -> String {
    let display: Vec<String> = data
        .iter()
        .take(max_bytes)
        .map(|b| format!("{b:02X}"))
        .collect();
    let s = display.join(" ");
    if data.len() > max_bytes {
        format!("{s}...")
    } else {
        s
    }
}

fn pct(part: u32, total: u32) -> f64 {
    if total == 0 {
        0.0
    } else {
        (part as f64 / total as f64) * 100.0
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
