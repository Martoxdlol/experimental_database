use dioxus::prelude::*;

use crate::state::AppState;

#[component]
pub fn KeyToolsModule() -> Element {
    let state = use_context::<AppState>();
    let db = state.db.read().clone();

    let Some(db) = db else {
        return rsx! { div { class: "empty-state", "No database open" } };
    };

    let db_decode = db.clone();
    let db_encode = db.clone();

    let mut decode_input: Signal<String> = use_signal(String::new);
    let mut decode_mode: Signal<String> = use_signal(|| "auto".to_string());
    let mut decode_result: Signal<Option<Result<String, String>>> = use_signal(|| None);

    let mut encode_mode: Signal<String> = use_signal(|| "primary".to_string());
    let mut encode_doc_id: Signal<String> = use_signal(String::new);
    let mut encode_ts: Signal<String> = use_signal(String::new);
    let mut encode_scalars: Signal<String> = use_signal(String::new);
    let mut encode_result: Signal<Option<Result<String, String>>> = use_signal(|| None);

    rsx! {
        div { class: "main-content",
            h2 { style: "margin-bottom: 16px;", "Key Encode / Decode" }

            div { style: "display: flex; gap: 16px; flex-wrap: wrap;",
                // Decode card
                div { style: "flex: 1; min-width: 400px;",
                    div { class: "card",
                        div { class: "card-header", "Decode" }
                        div { class: "card-body",
                            div { style: "margin-bottom: 8px;",
                                label { style: "color: var(--text-secondary); font-size: 11px; display: block;",
                                    "Hex bytes:"
                                }
                                textarea {
                                    class: "edit-input",
                                    style: "width: 100%; height: 60px; font-family: monospace; resize: vertical;",
                                    placeholder: "e.g. 00000000000000000000000000000001 FFFFFFFFFFFFFFFE",
                                    value: "{decode_input.read()}",
                                    oninput: move |e| decode_input.set(e.value()),
                                }
                            }
                            div { style: "display: flex; gap: 8px; margin-bottom: 8px; flex-wrap: wrap;",
                                for (val, label) in [("auto", "Auto"), ("primary", "Primary Key"), ("secondary", "Secondary Key"), ("cell", "Cell Value"), ("scalar", "Scalar")] {
                                    label { style: "font-size: 11px; color: var(--text-secondary);",
                                        input {
                                            r#type: "radio",
                                            name: "decode_mode",
                                            checked: *decode_mode.read() == val,
                                            onchange: move |_| decode_mode.set(val.to_string()),
                                        }
                                        " {label}"
                                    }
                                }
                            }
                            button {
                                class: "btn btn-action",
                                onclick: move |_| {
                                    let hex = decode_input.read().clone();
                                    let mode = decode_mode.read().clone();
                                    let result = db_decode.decode_key_bytes(&hex, &mode);
                                    decode_result.set(Some(result));
                                },
                                "Decode"
                            }

                            if let Some(result) = decode_result.read().as_ref() {
                                div { style: "margin-top: 12px;",
                                    match result {
                                        Ok(text) => rsx! {
                                            pre {
                                                class: "decode-result",
                                                style: "padding: 8px; background: var(--bg-primary); border: 1px solid var(--border); border-radius: 4px; font-size: 12px; white-space: pre-wrap; color: var(--checksum-ok);",
                                                "{text}"
                                            }
                                        },
                                        Err(e) => rsx! {
                                            div { style: "color: var(--write-danger); font-size: 12px;", "{e}" }
                                        },
                                    }
                                }
                            }
                        }
                    }
                }

                // Encode card
                div { style: "flex: 1; min-width: 400px;",
                    div { class: "card",
                        div { class: "card-header", "Encode" }
                        div { class: "card-body",
                            div { style: "display: flex; gap: 8px; margin-bottom: 8px;",
                                for (val, label) in [("primary", "Primary Key"), ("secondary", "Secondary Key"), ("scalar", "Scalar")] {
                                    label { style: "font-size: 11px; color: var(--text-secondary);",
                                        input {
                                            r#type: "radio",
                                            name: "encode_mode",
                                            checked: *encode_mode.read() == val,
                                            onchange: move |_| encode_mode.set(val.to_string()),
                                        }
                                        " {label}"
                                    }
                                }
                            }

                            if *encode_mode.read() == "primary" || *encode_mode.read() == "secondary" {
                                div { style: "margin-bottom: 8px;",
                                    label { style: "color: var(--text-secondary); font-size: 11px; display: block;",
                                        "DocId (hex, 32 chars):"
                                    }
                                    input {
                                        class: "edit-input",
                                        style: "width: 100%;",
                                        placeholder: "00000000000000000000000000000001",
                                        value: "{encode_doc_id.read()}",
                                        oninput: move |e| encode_doc_id.set(e.value()),
                                    }
                                }
                                div { style: "margin-bottom: 8px;",
                                    label { style: "color: var(--text-secondary); font-size: 11px; display: block;",
                                        "Timestamp:"
                                    }
                                    input {
                                        class: "edit-input",
                                        style: "width: 100%;",
                                        placeholder: "e.g. 42",
                                        value: "{encode_ts.read()}",
                                        oninput: move |e| encode_ts.set(e.value()),
                                    }
                                }
                            }
                            if *encode_mode.read() == "secondary" || *encode_mode.read() == "scalar" {
                                div { style: "margin-bottom: 8px;",
                                    label { style: "color: var(--text-secondary); font-size: 11px; display: block;",
                                        "Scalar values (JSON array):"
                                    }
                                    input {
                                        class: "edit-input",
                                        style: "width: 100%;",
                                        placeholder: r#"e.g. ["hello", 42, null]"#,
                                        value: "{encode_scalars.read()}",
                                        oninput: move |e| encode_scalars.set(e.value()),
                                    }
                                }
                            }

                            button {
                                class: "btn btn-action",
                                onclick: move |_| {
                                    let mode = encode_mode.read().clone();
                                    let doc_id = encode_doc_id.read().clone();
                                    let ts = encode_ts.read().clone();
                                    let scalars = encode_scalars.read().clone();
                                    let result = db_encode.encode_key_bytes(&mode, &doc_id, &ts, &scalars);
                                    encode_result.set(Some(result));
                                },
                                "Encode"
                            }

                            if let Some(result) = encode_result.read().as_ref() {
                                div { style: "margin-top: 12px;",
                                    match result {
                                        Ok(hex) => rsx! {
                                            pre {
                                                style: "padding: 8px; background: var(--bg-primary); border: 1px solid var(--border); border-radius: 4px; font-size: 12px; white-space: pre-wrap; color: var(--checksum-ok); word-break: break-all;",
                                                "{hex}"
                                            }
                                        },
                                        Err(e) => rsx! {
                                            div { style: "color: var(--write-danger); font-size: 12px;", "{e}" }
                                        },
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
