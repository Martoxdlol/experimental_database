use dioxus::prelude::*;

use exdb_core::field_path::FieldPath;
use exdb_query::{validate_range, encode_range};

use super::scan::parse_range_exprs;

#[component]
pub fn RangeToolsModule() -> Element {
    let mut fields_input: Signal<String> = use_signal(|| "age".to_string());
    let mut range_json: Signal<String> = use_signal(|| r#"[{"Gte": ["age", 18]}, {"Lt": ["age", 65]}]"#.to_string());
    let mut result: Signal<Option<RangeResult>> = use_signal(|| None);

    rsx! {
        div { class: "main-content",
            h2 { style: "margin-bottom: 16px;", "Range Tools" }

            div { class: "card",
                div { class: "card-header", "Range Validation & Encoding" }
                div { class: "card-body",
                    div { style: "display: flex; flex-direction: column; gap: 12px;",
                        div {
                            label { style: "color: var(--text-secondary); font-size: 11px; display: block; margin-bottom: 4px;",
                                "Index fields (comma-separated):"
                            }
                            div { style: "color: var(--text-secondary); font-size: 10px; margin-bottom: 4px;",
                                "e.g. \"country, age\" or \"user.name\" for nested paths"
                            }
                            input {
                                class: "edit-input",
                                style: "width: 100%;",
                                value: "{fields_input.read()}",
                                oninput: move |e| fields_input.set(e.value()),
                            }
                        }
                        div {
                            label { style: "color: var(--text-secondary); font-size: 11px; display: block; margin-bottom: 4px;",
                                "Range expressions (JSON array):"
                            }
                            textarea {
                                class: "edit-input",
                                style: "width: 100%; height: 80px; font-family: monospace; font-size: 12px; resize: vertical;",
                                value: "{range_json.read()}",
                                oninput: move |e| range_json.set(e.value()),
                            }
                        }
                        button {
                            class: "btn btn-action",
                            onclick: move |_| {
                                let fields_str = fields_input.read().clone();
                                let range_str = range_json.read().clone();
                                result.set(Some(evaluate_range(&fields_str, &range_str)));
                            },
                            "Validate & Encode"
                        }
                    }
                }
            }

            // Result card
            if let Some(res) = result.read().as_ref() {
                div { class: "card",
                    div { class: "card-header", "Result" }
                    div { class: "card-body",
                        if let Some(err) = &res.error {
                            div { style: "color: var(--page-deleted); font-size: 12px; padding: 8px;",
                                "{err}"
                            }
                        }
                        if let Some(shape) = &res.shape {
                            div { style: "margin-bottom: 12px;",
                                div { style: "font-weight: bold; font-size: 12px; margin-bottom: 6px; color: var(--text-primary);",
                                    "Validation (RangeShape)"
                                }
                                div { class: "console-output", style: "padding: 8px;",
                                    pre { style: "font-size: 12px; color: var(--text-primary); margin: 0;",
                                        "{shape}"
                                    }
                                }
                            }
                        }
                        if let Some(encoded) = &res.encoded {
                            div {
                                div { style: "font-weight: bold; font-size: 12px; margin-bottom: 6px; color: var(--text-primary);",
                                    "Encoded Byte Interval"
                                }
                                div { class: "console-output", style: "padding: 8px;",
                                    pre { style: "font-size: 12px; color: var(--text-primary); margin: 0; font-family: monospace;",
                                        "{encoded}"
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Reference card
            div { class: "card",
                div { class: "card-header", "Range Expression Reference" }
                div { class: "card-body",
                    pre { style: "font-size: 11px; color: var(--text-secondary); white-space: pre-wrap; margin: 0;",
                        r#"Format: [expr1, expr2, ...]
Each expr: {{"Op": ["field", value]}}

Operators:
  Eq   - equality (prefix)
  Gt   - greater than (exclusive lower bound)
  Gte  - greater or equal (inclusive lower bound)
  Lt   - less than (exclusive upper bound)
  Lte  - less or equal (inclusive upper bound)

Rules:
  - Eq predicates must come first (contiguous prefix)
  - Range bounds on the next field after Eq prefix
  - At most one lower + one upper bound per field
  - Fields must follow index order

Examples:
  Index [A, B]:
    [{{"Eq": ["A", 1]}}]                         prefix scan on A
    [{{"Eq": ["A", 1]}}, {{"Gte": ["B", 5]}}]     range on B within A=1
    [{{"Gte": ["A", 10]}}, {{"Lt": ["A", 20]}}]   bounded range on A"#
                    }
                }
            }
        }
    }
}

struct RangeResult {
    shape: Option<String>,
    encoded: Option<String>,
    error: Option<String>,
}

fn evaluate_range(fields_str: &str, range_str: &str) -> RangeResult {
    let index_fields: Vec<FieldPath> = fields_str
        .split(',')
        .map(|s| {
            let s = s.trim();
            let segments: Vec<String> = s.split('.').map(|p| p.to_string()).collect();
            if segments.len() == 1 {
                FieldPath::single(&segments[0])
            } else {
                FieldPath::new(segments)
            }
        })
        .collect();

    let range_exprs = match parse_range_exprs(range_str) {
        Ok(r) => r,
        Err(e) => return RangeResult { shape: None, encoded: None, error: Some(e) },
    };

    // Validate
    let shape = match validate_range(&index_fields, &range_exprs) {
        Ok(s) => s,
        Err(e) => return RangeResult {
            shape: None,
            encoded: None,
            error: Some(format!("Validation error: {e:?}")),
        },
    };

    let shape_str = format!(
        "eq_count: {}\nrange_field: {}\nhas_lower: {}\nhas_upper: {}",
        shape.eq_count,
        shape.range_field.map(|i| i.to_string()).unwrap_or("None".into()),
        shape.has_lower,
        shape.has_upper,
    );

    // Encode
    let encoded_str = match encode_range(&index_fields, &range_exprs) {
        Ok((lower, upper)) => {
            let lower_str = format_bound(&lower);
            let upper_str = format_bound(&upper);
            format!("lower: {lower_str}\nupper: {upper_str}")
        }
        Err(e) => return RangeResult {
            shape: Some(shape_str),
            encoded: None,
            error: Some(format!("Encoding error: {e:?}")),
        },
    };

    RangeResult {
        shape: Some(shape_str),
        encoded: Some(encoded_str),
        error: None,
    }
}

fn format_bound(bound: &std::ops::Bound<Vec<u8>>) -> String {
    match bound {
        std::ops::Bound::Unbounded => "Unbounded".into(),
        std::ops::Bound::Included(v) => format!("Included({})", hex_encode(v)),
        std::ops::Bound::Excluded(v) => format!("Excluded({})", hex_encode(v)),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
