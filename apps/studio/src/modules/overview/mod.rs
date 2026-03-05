use dioxus::prelude::*;

use crate::state::{AppState, OperationResult};

#[component]
pub fn OverviewModule() -> Element {
    let mut state = use_context::<AppState>();
    let db = state.db.read().clone();
    let write_enabled = *state.write_enabled.read();

    let Some(db) = db else {
        return rsx! {
            div { class: "empty-state",
                div { class: "empty-state-icon", "\u{1F4CA}" }
                "Open a database to see the overview"
            }
        };
    };

    let fh = db.read_file_header();

    // Count pages by type
    let db_for_scan = db.clone();
    let type_counts = use_resource(move || {
        let db = db_for_scan.clone();
        async move {
            tokio::task::spawn_blocking(move || db.scan_page_types())
                .await
                .ok()
                .and_then(|r| r.ok())
        }
    });

    // Editing state
    let mut editing_field: Signal<Option<String>> = use_signal(|| None);
    let mut edit_value: Signal<String> = use_signal(|| String::new());

    rsx! {
        div { class: "main-content",
            h2 { style: "margin-bottom: 16px;", "Database Overview" }

            // File Header card
            div { class: "card",
                div { class: "card-header", "File Header" }
                div { class: "card-body",
                    table { class: "kv-table",
                        {header_row("Magic", format!("EXDB (0x{:08X})", fh.magic.get()), None, false, &mut editing_field, &mut edit_value, &mut state)}
                        {header_row("Version", format!("{}", fh.version.get()), None, false, &mut editing_field, &mut edit_value, &mut state)}
                        {header_row("Page Size", format!("{} bytes", fh.page_size.get()), None, false, &mut editing_field, &mut edit_value, &mut state)}
                        {header_row("Page Count", format!("{}", fh.page_count.get()), Some("page_count"), write_enabled, &mut editing_field, &mut edit_value, &mut state)}
                        {header_row("Free List Head", format!("Page #{}", fh.free_list_head.get()), Some("free_list_head"), write_enabled, &mut editing_field, &mut edit_value, &mut state)}
                        {header_row("Catalog Root", format!("Page #{}", fh.catalog_root_page.get()), Some("catalog_root_page"), write_enabled, &mut editing_field, &mut edit_value, &mut state)}
                        {header_row("Name Root", format!("Page #{}", fh.catalog_name_root_page.get()), Some("catalog_name_root_page"), write_enabled, &mut editing_field, &mut edit_value, &mut state)}
                        {header_row("Checkpoint LSN", format!("{}", fh.checkpoint_lsn.get()), Some("checkpoint_lsn"), write_enabled, &mut editing_field, &mut edit_value, &mut state)}
                        {header_row("Visible TS", format!("{}", fh.visible_ts.get()), Some("visible_ts"), write_enabled, &mut editing_field, &mut edit_value, &mut state)}
                        {header_row("Generation", format!("{}", fh.generation.get()), Some("generation"), write_enabled, &mut editing_field, &mut edit_value, &mut state)}
                        {header_row("Next Collection ID", format!("{}", fh.next_collection_id.get()), Some("next_collection_id"), write_enabled, &mut editing_field, &mut edit_value, &mut state)}
                        {header_row("Next Index ID", format!("{}", fh.next_index_id.get()), Some("next_index_id"), write_enabled, &mut editing_field, &mut edit_value, &mut state)}
                        {header_row("Created", format_timestamp(fh.created_at.get()), None, false, &mut editing_field, &mut edit_value, &mut state)}
                    }
                }
            }

            // Space Usage card
            if let Some(Some(pages)) = type_counts.read().as_ref() {
                {
                    let mut btree_internal = 0u32;
                    let mut btree_leaf = 0u32;
                    let mut heap_count = 0u32;
                    let mut overflow_count = 0u32;
                    let mut free_count = 0u32;
                    let mut header_count = 0u32;
                    let mut other = 0u32;
                    let total = pages.len() as u32;

                    for (_, pt, _) in pages {
                        match pt {
                            Some(exdb_storage::page::PageType::BTreeInternal) => btree_internal += 1,
                            Some(exdb_storage::page::PageType::BTreeLeaf) => btree_leaf += 1,
                            Some(exdb_storage::page::PageType::Heap) => heap_count += 1,
                            Some(exdb_storage::page::PageType::Overflow) => overflow_count += 1,
                            Some(exdb_storage::page::PageType::Free) => free_count += 1,
                            Some(exdb_storage::page::PageType::FileHeader) | Some(exdb_storage::page::PageType::FileHeaderShadow) => header_count += 1,
                            _ => other += 1,
                        }
                    }

                    rsx! {
                        div { class: "card",
                            div { class: "card-header", "Space Usage" }
                            div { class: "card-body",
                                {space_bar_row("BTree Internal", btree_internal, total, "var(--page-header)")}
                                {space_bar_row("BTree Leaf", btree_leaf, total, "var(--page-cells)")}
                                {space_bar_row("Heap", heap_count, total, "#bb9af7")}
                                {space_bar_row("Overflow", overflow_count, total, "#f7768e")}
                                {space_bar_row("Free", free_count, total, "var(--page-free)")}
                                {space_bar_row("FileHeader", header_count, total, "#73daca")}
                                if other > 0 {
                                    {space_bar_row("Other", other, total, "var(--text-secondary)")}
                                }
                            }
                        }
                    }
                }
            }

            // WAL Summary card
            {
                let wal_info = db.wal_info();
                rsx! {
                    div { class: "card",
                        div { class: "card-header", "WAL Summary" }
                        div { class: "card-body",
                            table { class: "kv-table",
                                tr {
                                    td { "Current LSN" }
                                    td { span { class: "kv-value", "{wal_info.current_lsn}" } }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn header_row(
    label: &str,
    value: String,
    field_name: Option<&str>,
    write_enabled: bool,
    editing_field: &mut Signal<Option<String>>,
    edit_value: &mut Signal<String>,
    state: &mut AppState,
) -> Element {
    let label = label.to_string();
    let field_name = field_name.map(|s| s.to_string());
    let is_editing = editing_field
        .read()
        .as_ref()
        .map(|f| Some(f.clone()) == field_name)
        .unwrap_or(false);
    let mut editing_field = *editing_field;
    let mut edit_value = *edit_value;
    let mut state = *state;

    if is_editing {
        let ev = edit_value.read().clone();
        let fn_clone = field_name.clone().unwrap_or_default();
        rsx! {
            tr {
                td { "{label}" }
                td {
                    input {
                        class: "edit-input",
                        value: "{ev}",
                        oninput: move |e| edit_value.set(e.value()),
                        onkeydown: move |e| {
                            if e.key() == Key::Enter {
                                let val_str = edit_value.read().clone();
                                let fn_c = fn_clone.clone();
                                if let Ok(val) = val_str.parse::<u64>() {
                                    let db = state.db.read().clone();
                                    if let Some(db) = db {
                                        match db.update_file_header_field(&fn_c, val) {
                                            Ok(()) => state.last_result.set(Some(OperationResult::Success(
                                                format!("Updated {fn_c} to {val}")
                                            ))),
                                            Err(e) => state.last_result.set(Some(OperationResult::Error(
                                                e.to_string()
                                            ))),
                                        }
                                    }
                                }
                                editing_field.set(None);
                            } else if e.key() == Key::Escape {
                                editing_field.set(None);
                            }
                        },
                    }
                }
            }
        }
    } else {
        rsx! {
            tr {
                td { "{label}" }
                td {
                    span { class: "kv-value", "{value}" }
                    if field_name.is_some() && write_enabled {
                        button {
                            class: "edit-btn",
                            onclick: move |_| {
                                editing_field.set(field_name.clone());
                                edit_value.set(String::new());
                            },
                            "\u{270E}"
                        }
                    }
                }
            }
        }
    }
}

fn space_bar_row(label: &str, count: u32, total: u32, color: &str) -> Element {
    let pct = if total > 0 {
        (count as f64 / total as f64) * 100.0
    } else {
        0.0
    };
    let bar_width = format!("{pct:.0}%");
    let label = label.to_string();
    let color = color.to_string();

    rsx! {
        div {
            style: "display: flex; align-items: center; gap: 12px; margin-bottom: 6px;",
            div {
                style: "flex: 1; height: 18px; background: var(--bg-primary); border-radius: 3px; overflow: hidden;",
                div {
                    style: "height: 100%; width: {bar_width}; background: {color}; border-radius: 3px; transition: width 0.3s;",
                }
            }
            span {
                style: "color: var(--text-secondary); font-size: 11px; min-width: 160px;",
                "{label} ({count} pages, {pct:.0}%)"
            }
        }
    }
}

fn format_timestamp(ms: u64) -> String {
    if ms == 0 {
        return "N/A".to_string();
    }
    format!("{ms} (epoch ms)")
}
