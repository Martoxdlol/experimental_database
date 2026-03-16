use dioxus::prelude::*;

use crate::components::HexDump;
use crate::state::{AppState, OperationResult};

#[component]
pub fn WalModule() -> Element {
    let state = use_context::<AppState>();
    let db = state.db.read().clone();
    let write_enabled = *state.write_enabled.read();

    let Some(db) = db else {
        return rsx! { div { class: "empty-state", "No database open" } };
    };

    let mut from_lsn: Signal<u64> = use_signal(|| 0);
    let mut selected_frame: Signal<Option<usize>> = use_signal(|| None);

    let frames = use_resource(move || {
        let _rev = *state.revision.read();
        let db = db.clone();
        let lsn = *from_lsn.read();
        async move {
            Some(db.read_wal_frames(lsn, 500).await)
        }
    });

    rsx! {
        div { class: "master-detail",
            // Frame list (left panel)
            div { class: "master-panel",
                div { style: "padding: 8px;",
                    label { style: "color: var(--text-secondary); font-size: 11px;", "From LSN:" }
                    input {
                        class: "filter-input",
                        value: "{from_lsn.read()}",
                        oninput: move |e| {
                            if let Ok(v) = e.value().parse::<u64>() {
                                from_lsn.set(v);
                            }
                        },
                    }
                }

                if let Some(Some(frames)) = frames.read().as_ref() {
                    div { style: "padding: 4px 8px; color: var(--text-secondary); font-size: 11px;",
                        "{frames.len()} frames"
                    }
                    div { style: "overflow-y: auto;",
                        for (i, frame) in frames.iter().enumerate() {
                            {
                                let sel = *selected_frame.read() == Some(i);
                                let idx = i;
                                rsx! {
                                    div {
                                        class: if sel { "list-item selected" } else { "list-item" },
                                        onclick: move |_| selected_frame.set(Some(idx)),
                                        span { style: "color: var(--text-secondary); min-width: 60px; font-size: 11px;",
                                            "LSN {frame.lsn}"
                                        }
                                        span { style: "font-size: 10px;",
                                            "0x{frame.record_type:02X}"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Frame detail (right panel)
            div { class: "detail-panel",
                if let Some(sel_idx) = *selected_frame.read() {
                    if let Some(Some(frames)) = frames.read().as_ref() {
                        if let Some(frame) = frames.get(sel_idx) {
                            div {
                                h3 { style: "margin-bottom: 12px;",
                                    "Frame @ LSN {frame.lsn}"
                                }

                                div { class: "card",
                                    div { class: "card-header", "Record" }
                                    div { class: "card-body",
                                        table { class: "kv-table",
                                            tr {
                                                td { "LSN" }
                                                td { span { class: "kv-value", "{frame.lsn}" } }
                                            }
                                            tr {
                                                td { "Record Type" }
                                                td { span { class: "kv-value", "0x{frame.record_type:02X} ({frame.record_type_name})" } }
                                            }
                                            tr {
                                                td { "Payload Length" }
                                                td { span { class: "kv-value", "{frame.payload.len()} bytes" } }
                                            }
                                        }
                                    }
                                }

                                if !frame.payload.is_empty() {
                                    div { class: "card",
                                        div { class: "card-header", "Payload Hex" }
                                        div { class: "card-body",
                                            HexDump { data: frame.payload.clone() }
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else {
                    div { class: "empty-state",
                        div { class: "empty-state-icon", "\u{1F4DD}" }
                        "Select a WAL frame to inspect"
                    }
                }
            }
        }

        // Append WAL record (if write enabled)
        if write_enabled {
            WalAppendForm {}
        }
    }
}

#[component]
fn WalAppendForm() -> Element {
    let mut state = use_context::<AppState>();
    let mut record_type: Signal<String> = use_signal(|| "01".to_string());
    let mut payload_hex: Signal<String> = use_signal(String::new);

    rsx! {
        div {
            style: "position: fixed; bottom: 16px; left: 80px; right: 16px;",
            div { class: "card",
                div { class: "card-header", "Append WAL Record" }
                div { class: "card-body",
                    div { style: "display: flex; gap: 8px; align-items: flex-end;",
                        div {
                            label { style: "color: var(--text-secondary); font-size: 11px; display: block;", "Record Type (hex):" }
                            input {
                                class: "edit-input",
                                style: "width: 80px;",
                                value: "{record_type.read()}",
                                oninput: move |e| record_type.set(e.value()),
                            }
                        }
                        div { style: "flex: 1;",
                            label { style: "color: var(--text-secondary); font-size: 11px; display: block;", "Payload (hex):" }
                            input {
                                class: "edit-input",
                                style: "width: 100%;",
                                placeholder: "e.g. 0102030405",
                                value: "{payload_hex.read()}",
                                oninput: move |e| payload_hex.set(e.value()),
                            }
                        }
                        button {
                            class: "btn btn-action",
                            onclick: move |_| {
                                let rt_str = record_type.read().clone();
                                let pl_str = payload_hex.read().clone();
                                let rt = u8::from_str_radix(&rt_str, 16).unwrap_or(0);
                                let payload = parse_hex_bytes(&pl_str).unwrap_or_default();
                                let db = state.db.read().clone();
                                spawn(async move {
                                    if let Some(db) = db {
                                        match db.append_wal(rt, &payload).await {
                                            Ok(lsn) => {
                                                state.last_result.set(Some(OperationResult::Success(
                                                    format!("Appended WAL record at LSN {lsn}")
                                                )));
                                                state.notify_mutation();
                                                payload_hex.set(String::new());
                                            }
                                            Err(e) => {
                                                state.last_result.set(Some(OperationResult::Error(e.to_string())));
                                            }
                                        }
                                    }
                                });
                            },
                            "Append"
                        }
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
