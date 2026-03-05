use dioxus::prelude::*;

use crate::state::AppState;

#[derive(Clone, Debug)]
struct ConsoleLine {
    command: String,
    result: String,
    success: bool,
}

#[component]
pub fn ConsoleModule() -> Element {
    let state = use_context::<AppState>();
    let db = state.db.read().clone();
    let _write_enabled = *state.write_enabled.read();

    let Some(_db) = db else {
        return rsx! { div { class: "empty-state", "No database open" } };
    };

    let mut target: Signal<String> = use_signal(|| "storage".to_string());
    let mut method: Signal<String> = use_signal(|| "file_header".to_string());
    let mut param1: Signal<String> = use_signal(|| String::new());
    let mut param2: Signal<String> = use_signal(|| String::new());
    let mut history: Signal<Vec<ConsoleLine>> = use_signal(|| Vec::new());

    let methods_for_target = match target.read().as_str() {
        "storage" => vec![
            "file_header",
            "page_count",
            "create_btree",
            "checkpoint",
        ],
        "btree" => vec!["scan", "get", "insert", "delete"],
        "page" => vec!["read", "compact", "stamp_checksum"],
        "freelist" => vec!["walk", "allocate", "deallocate"],
        "heap" => vec!["load", "store", "free"],
        _ => vec![],
    };

    rsx! {
        div { class: "main-content",
            h2 { style: "margin-bottom: 16px;", "Engine Console" }

            // Command input
            div { class: "card",
                div { class: "card-header", "Command" }
                div { class: "card-body",
                    div { style: "display: flex; gap: 8px; align-items: flex-end; flex-wrap: wrap;",
                        div {
                            label { style: "color: var(--text-secondary); font-size: 11px; display: block;", "Target:" }
                            select {
                                class: "select",
                                value: "{target.read()}",
                                onchange: move |e| {
                                    target.set(e.value());
                                    method.set(String::new());
                                },
                                option { value: "storage", "storage" }
                                option { value: "btree", "btree(root)" }
                                option { value: "page", "page(id)" }
                                option { value: "freelist", "freelist" }
                                option { value: "heap", "heap" }
                            }
                        }
                        div {
                            label { style: "color: var(--text-secondary); font-size: 11px; display: block;", "Method:" }
                            select {
                                class: "select",
                                value: "{method.read()}",
                                onchange: move |e| method.set(e.value()),
                                for m in &methods_for_target {
                                    option { value: "{m}", "{m}" }
                                }
                            }
                        }
                        div {
                            label { style: "color: var(--text-secondary); font-size: 11px; display: block;", "Param 1:" }
                            input {
                                class: "edit-input",
                                style: "width: 120px;",
                                value: "{param1.read()}",
                                oninput: move |e| param1.set(e.value()),
                            }
                        }
                        div {
                            label { style: "color: var(--text-secondary); font-size: 11px; display: block;", "Param 2:" }
                            input {
                                class: "edit-input",
                                style: "width: 120px;",
                                value: "{param2.read()}",
                                oninput: move |e| param2.set(e.value()),
                            }
                        }
                        button {
                            class: "btn btn-action",
                            onclick: move |_| {
                                let tgt = target.read().clone();
                                let mth = method.read().clone();
                                let p1 = param1.read().clone();
                                let p2 = param2.read().clone();
                                let db = state.db.read().clone();

                                let cmd = format!("{tgt}.{mth}({p1}{sep}{p2})",
                                    sep = if p2.is_empty() { "" } else { ", " });

                                if let Some(db) = db {
                                    let result = execute_command(&db, &tgt, &mth, &p1, &p2);
                                    let (result_text, success) = match result {
                                        Ok(s) => (s, true),
                                        Err(e) => (e, false),
                                    };
                                    history.write().push(ConsoleLine {
                                        command: cmd,
                                        result: result_text,
                                        success,
                                    });
                                }
                            },
                            "Execute"
                        }
                    }
                }
            }

            // Output log
            div { class: "card",
                div { class: "card-header",
                    "Output"
                    button {
                        class: "btn",
                        style: "margin-left: auto; font-size: 10px; padding: 2px 6px;",
                        onclick: move |_| history.write().clear(),
                        "Clear"
                    }
                }
                div { class: "card-body",
                    div { class: "console-output",
                        if history.read().is_empty() {
                            div { style: "color: var(--text-secondary);", "No commands executed yet" }
                        }
                        for line in history.read().iter().rev() {
                            div { class: "console-line",
                                div { class: "console-cmd", "> {line.command}" }
                                div {
                                    class: if line.success { "console-ok" } else { "console-err" },
                                    if line.success { "\u{2713} " } else { "\u{2717} " }
                                    "{line.result}"
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn execute_command(
    db: &crate::engine::DbHandle,
    target: &str,
    method: &str,
    p1: &str,
    p2: &str,
) -> Result<String, String> {
    match (target, method) {
        ("storage", "file_header") => {
            let fh = db.read_file_header();
            Ok(format!(
                "FileHeader {{ magic: 0x{:08X}, version: {}, page_size: {}, page_count: {}, \
                 free_list_head: {}, catalog_root: {}, checkpoint_lsn: {}, generation: {} }}",
                fh.magic.get(),
                fh.version.get(),
                fh.page_size.get(),
                fh.page_count.get(),
                fh.free_list_head.get(),
                fh.catalog_root_page.get(),
                fh.checkpoint_lsn.get(),
                fh.generation.get(),
            ))
        }
        ("storage", "page_count") => Ok(format!("{}", db.page_count())),
        ("storage", "create_btree") => {
            db.create_btree()
                .map(|root| format!("BTreeHandle {{ root_page: {root} }}"))
                .map_err(|e| e.to_string())
        }
        ("storage", "checkpoint") => Err("Use async checkpoint from toolbar".into()),
        ("btree", "scan") => {
            let root = p1.parse::<u32>().map_err(|_| "Invalid root page".to_string())?;
            let limit = p2.parse::<usize>().unwrap_or(20);
            let entries = db.btree_scan(root, limit).map_err(|e| e.to_string())?;
            let lines: Vec<String> = entries
                .iter()
                .map(|(k, v)| format!("  {} -> {} bytes", hex_short(k), v.len()))
                .collect();
            Ok(format!("{} entries:\n{}", entries.len(), lines.join("\n")))
        }
        ("btree", "get") => {
            let root = p1.parse::<u32>().map_err(|_| "Invalid root page".to_string())?;
            let key = parse_hex(p2).ok_or("Invalid hex key")?;
            match db.btree_get(root, &key).map_err(|e| e.to_string())? {
                Some(v) => Ok(format!("{} bytes: {}", v.len(), hex_short(&v))),
                None => Ok("Not found".into()),
            }
        }
        ("btree", "insert") => {
            let root = p1.parse::<u32>().map_err(|_| "Invalid root page".to_string())?;
            // p2 = "key:value" in hex
            let parts: Vec<&str> = p2.splitn(2, ':').collect();
            if parts.len() != 2 {
                return Err("Format: key_hex:value_hex".into());
            }
            let key = parse_hex(parts[0]).ok_or("Invalid hex key")?;
            let value = parse_hex(parts[1]).ok_or("Invalid hex value")?;
            db.btree_insert(root, &key, &value)
                .map(|_| "OK".into())
                .map_err(|e| e.to_string())
        }
        ("btree", "delete") => {
            let root = p1.parse::<u32>().map_err(|_| "Invalid root page".to_string())?;
            let key = parse_hex(p2).ok_or("Invalid hex key")?;
            db.btree_delete(root, &key)
                .map(|existed| if existed { "Deleted" } else { "Key not found" }.into())
                .map_err(|e| e.to_string())
        }
        ("page", "read") => {
            let page_id = p1.parse::<u32>().map_err(|_| "Invalid page ID".to_string())?;
            let info = db.read_page(page_id).map_err(|e| e.to_string())?;
            let type_name = info
                .page_type
                .map(|pt| format!("{pt:?}"))
                .unwrap_or("Unknown".into());
            Ok(format!(
                "Page #{page_id}: type={type_name}, slots={}, free={}, checksum={}",
                info.num_slots,
                info.free_space,
                if info.checksum_valid { "OK" } else { "BAD" }
            ))
        }
        ("page", "compact") => {
            let page_id = p1.parse::<u32>().map_err(|_| "Invalid page ID".to_string())?;
            db.compact_page(page_id)
                .map(|_| "Compacted".into())
                .map_err(|e| e.to_string())
        }
        ("page", "stamp_checksum") => {
            let page_id = p1.parse::<u32>().map_err(|_| "Invalid page ID".to_string())?;
            db.stamp_checksum(page_id)
                .map(|_| "Checksum stamped".into())
                .map_err(|e| e.to_string())
        }
        ("freelist", "walk") => {
            let info = db.walk_free_list().map_err(|e| e.to_string())?;
            if info.chain.is_empty() {
                Ok("Free list is empty".into())
            } else {
                let chain: Vec<String> = info.chain.iter().map(|p| format!("#{p}")).collect();
                Ok(format!("{} pages: {}", info.chain.len(), chain.join(" -> ")))
            }
        }
        ("freelist", "allocate") => {
            db.free_list_allocate()
                .map(|id| format!("Allocated page #{id}"))
                .map_err(|e| e.to_string())
        }
        ("freelist", "deallocate") => {
            let page_id = p1.parse::<u32>().map_err(|_| "Invalid page ID".to_string())?;
            db.free_list_deallocate(page_id)
                .map(|_| format!("Deallocated page #{page_id}"))
                .map_err(|e| e.to_string())
        }
        ("heap", "load") => {
            let page_id = p1.parse::<u32>().map_err(|_| "Invalid page ID".to_string())?;
            let slot_id = p2.parse::<u16>().map_err(|_| "Invalid slot ID".to_string())?;
            let data = db.heap_load(page_id, slot_id).map_err(|e| e.to_string())?;
            if let Ok(text) = std::str::from_utf8(&data) {
                Ok(format!("{} bytes: {}", data.len(), &text[..text.len().min(200)]))
            } else {
                Ok(format!("{} bytes: {}", data.len(), hex_short(&data)))
            }
        }
        ("heap", "store") => {
            let data = if p1.starts_with('{') || p1.starts_with('[') || p1.starts_with('"') {
                p1.as_bytes().to_vec()
            } else {
                parse_hex(p1).ok_or("Invalid hex data")?
            };
            let (page_id, slot_id) = db.heap_store(&data).map_err(|e| e.to_string())?;
            Ok(format!("HeapRef {{ page: {page_id}, slot: {slot_id} }}"))
        }
        ("heap", "free") => {
            let page_id = p1.parse::<u32>().map_err(|_| "Invalid page ID".to_string())?;
            let slot_id = p2.parse::<u16>().map_err(|_| "Invalid slot ID".to_string())?;
            db.heap_free(page_id, slot_id)
                .map(|_| "Freed".into())
                .map_err(|e| e.to_string())
        }
        _ => Err(format!("Unknown command: {target}.{method}")),
    }
}

fn hex_short(data: &[u8]) -> String {
    if data.len() <= 16 {
        data.iter()
            .map(|b| format!("{b:02X}"))
            .collect::<Vec<_>>()
            .join(" ")
    } else {
        let head: String = data[..8]
            .iter()
            .map(|b| format!("{b:02X}"))
            .collect::<Vec<_>>()
            .join(" ");
        format!("{head}... ({} bytes)", data.len())
    }
}

fn parse_hex(s: &str) -> Option<Vec<u8>> {
    let s = s.replace(' ', "");
    if s.is_empty() {
        return Some(Vec::new());
    }
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}
