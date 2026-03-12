use dioxus::prelude::*;

use exdb_query::filter_matches;

use super::scan::parse_filter;

#[component]
pub fn FilterTestModule() -> Element {
    let mut doc_json: Signal<String> = use_signal(|| r#"{"name": "alice", "age": 30, "active": true}"#.to_string());
    let mut filter_json: Signal<String> = use_signal(|| r#"{"And": [{"Gte": ["age", 18]}, {"Eq": ["active", true]}]}"#.to_string());
    let mut result: Signal<Option<Result<bool, String>>> = use_signal(|| None);

    rsx! {
        div { class: "main-content",
            h2 { style: "margin-bottom: 16px;", "Post-Filter Tester" }

            div { class: "card",
                div { class: "card-header", "Filter Evaluation" }
                div { class: "card-body",
                    div { style: "display: flex; flex-direction: column; gap: 12px;",
                        div {
                            label { style: "color: var(--text-secondary); font-size: 11px; display: block; margin-bottom: 4px;",
                                "Document (JSON):"
                            }
                            textarea {
                                class: "edit-input",
                                style: "width: 100%; height: 100px; font-family: monospace; font-size: 12px; resize: vertical;",
                                value: "{doc_json.read()}",
                                oninput: move |e| doc_json.set(e.value()),
                            }
                        }
                        div {
                            label { style: "color: var(--text-secondary); font-size: 11px; display: block; margin-bottom: 4px;",
                                "Filter expression (JSON):"
                            }
                            div { style: "color: var(--text-secondary); font-size: 10px; margin-bottom: 4px;",
                                "Operators: Eq, Ne, Gt, Gte, Lt, Lte, In, And, Or, Not"
                            }
                            textarea {
                                class: "edit-input",
                                style: "width: 100%; height: 60px; font-family: monospace; font-size: 12px; resize: vertical;",
                                value: "{filter_json.read()}",
                                oninput: move |e| filter_json.set(e.value()),
                            }
                        }
                        button {
                            class: "btn btn-action",
                            onclick: move |_| {
                                let doc_str = doc_json.read().clone();
                                let filt_str = filter_json.read().clone();
                                result.set(Some(evaluate_filter(&doc_str, &filt_str)));
                            },
                            "Evaluate"
                        }
                    }
                }
            }

            // Result card
            if let Some(res) = result.read().as_ref() {
                div { class: "card",
                    div { class: "card-header", "Result" }
                    div { class: "card-body",
                        match res {
                            Ok(true) => rsx! {
                                div { style: "font-size: 16px; color: #4EC9B0; font-weight: bold; padding: 12px;",
                                    "MATCH (true)"
                                }
                            },
                            Ok(false) => rsx! {
                                div { style: "font-size: 16px; color: var(--page-deleted); font-weight: bold; padding: 12px;",
                                    "NO MATCH (false)"
                                }
                            },
                            Err(e) => rsx! {
                                div { style: "color: var(--page-deleted); font-size: 12px; padding: 12px;",
                                    "{e}"
                                }
                            },
                        }
                    }
                }
            }

            // Reference card
            div { class: "card",
                div { class: "card-header", "Filter Reference" }
                div { class: "card-body",
                    pre { style: "font-size: 11px; color: var(--text-secondary); white-space: pre-wrap; margin: 0;",
                        r#"Comparison:  {{"Eq": ["field", value]}}
             {{"Ne": ["field", value]}}
             {{"Gt": ["field", value]}}
             {{"Gte": ["field", value]}}
             {{"Lt": ["field", value]}}
             {{"Lte": ["field", value]}}
Set:         {{"In": ["field", [val1, val2, ...]]}}
Compound:    {{"And": [filter1, filter2, ...]}}
             {{"Or": [filter1, filter2, ...]}}
Negation:    {{"Not": filter}}

Values: integers (5), floats (3.14), strings ("abc"), booleans (true/false), null
Fields: "name", nested "user.age"
Type-strict: int64(5) != float64(5.0)"#
                    }
                }
            }
        }
    }
}

fn evaluate_filter(doc_json: &str, filter_json: &str) -> Result<bool, String> {
    let doc: serde_json::Value = serde_json::from_str(doc_json)
        .map_err(|e| format!("Invalid document JSON: {e}"))?;
    let filter = parse_filter(filter_json)?;
    Ok(filter_matches(&doc, &filter))
}
