use dioxus::prelude::*;

use exdb::ScanDirection;

use crate::state::AppState;

#[derive(Clone, Debug)]
struct QueryResultInfo {
    docs: Vec<serde_json::Value>,
    error: Option<String>,
    elapsed_ms: u128,
}

#[component]
pub fn QueryWorkbenchModule() -> Element {
    let state = use_context::<AppState>();
    let _rev = *state.revision.read();

    let engine = state.engine.read().clone();
    let collections = engine
        .as_ref()
        .map(|e| e.l6.list_collections())
        .unwrap_or_default();

    let mut collection = use_signal(|| String::new());
    let mut index = use_signal(|| "_created_at".to_string());
    let mut direction = use_signal(|| ScanDirection::Forward);
    let mut limit = use_signal(|| 50usize);
    let mut result: Signal<Option<QueryResultInfo>> = use_signal(|| None);
    let mut json_mode = use_signal(|| false);

    // Auto-select first collection
    if collection.read().is_empty() && !collections.is_empty() {
        collection.set(collections[0].name.clone());
    }

    let dir = *direction.read();

    rsx! {
        div { class: "main-content",
            div { class: "card",
                h3 { "Query Workbench" }
                div { class: "form-grid",
                    div { class: "form-row",
                        label { "Collection:" }
                        select {
                            class: "input",
                            value: "{collection.read()}",
                            onchange: move |e| collection.set(e.value()),
                            for col in &collections {
                                option { value: "{col.name}", "{col.name}" }
                            }
                        }
                    }
                    div { class: "form-row",
                        label { "Index:" }
                        input {
                            class: "input",
                            value: "{index.read()}",
                            oninput: move |e| index.set(e.value()),
                            placeholder: "_created_at",
                        }
                    }
                    div { class: "form-row",
                        label { "Direction:" }
                        select {
                            class: "input",
                            onchange: move |e| {
                                direction.set(if e.value() == "forward" {
                                    ScanDirection::Forward
                                } else {
                                    ScanDirection::Backward
                                });
                            },
                            option { value: "forward", selected: dir == ScanDirection::Forward, "Forward" }
                            option { value: "backward", selected: dir == ScanDirection::Backward, "Backward" }
                        }
                        label { "Limit:" }
                        input {
                            class: "input input-small",
                            r#type: "number",
                            value: "{limit.read()}",
                            oninput: move |e| {
                                if let Ok(n) = e.value().parse::<usize>() {
                                    limit.set(n.max(1).min(10000));
                                }
                            },
                        }
                    }
                }

                div { class: "form-row", style: "margin-top: 12px;",
                    button {
                        class: "btn btn-action",
                        disabled: collection.read().is_empty(),
                        onclick: move |_| {
                            let coll = collection.read().clone();
                            let idx = index.read().clone();
                            let dir = *direction.read();
                            let lim = *limit.read();
                            spawn(async move {
                                let engine = state.engine.read().clone();
                                let Some(engine) = engine else { return };
                                let start = std::time::Instant::now();

                                match engine
                                    .l6
                                    .query_docs(&coll, &idx, &[], None, Some(dir), Some(lim))
                                    .await
                                {
                                    Ok(docs) => {
                                        result.set(Some(QueryResultInfo {
                                            docs,
                                            error: None,
                                            elapsed_ms: start.elapsed().as_millis(),
                                        }));
                                    }
                                    Err(e) => {
                                        result.set(Some(QueryResultInfo {
                                            docs: vec![],
                                            error: Some(format!("{e}")),
                                            elapsed_ms: start.elapsed().as_millis(),
                                        }));
                                    }
                                }
                            });
                        },
                        "Execute Query"
                    }
                }
            }

            if let Some(res) = result.read().as_ref() {
                div { class: "card",
                    div { class: "form-row",
                        h4 { "Results ({res.docs.len()} docs, {res.elapsed_ms}ms)" }
                        button {
                            class: "btn btn-small",
                            onclick: move |_| json_mode.set(!json_mode.cloned()),
                            if *json_mode.read() { "Table View" } else { "JSON View" }
                        }
                    }

                    if let Some(ref err) = res.error {
                        div { class: "error-message", "{err}" }
                    }

                    if res.docs.is_empty() && res.error.is_none() {
                        div { class: "empty-state", "No results" }
                    }

                    if *json_mode.read() {
                        pre { class: "doc-json",
                            "{serde_json::to_string_pretty(&res.docs).unwrap_or_default()}"
                        }
                    } else {
                        if !res.docs.is_empty() {
                            {render_table(&res.docs)}
                        }
                    }
                }
            }
        }
    }
}

fn render_table(docs: &[serde_json::Value]) -> Element {
    let columns: Vec<String> = if let Some(serde_json::Value::Object(map)) = docs.first() {
        map.keys().cloned().collect()
    } else {
        vec![]
    };

    rsx! {
        div { class: "table-scroll",
            table { class: "data-table",
                thead {
                    tr {
                        for col in &columns {
                            th { "{col}" }
                        }
                    }
                }
                tbody {
                    for doc in docs {
                        tr {
                            for col in &columns {
                                td { class: "mono",
                                    {cell_value(doc.get(col.as_str()))}
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn cell_value(v: Option<&serde_json::Value>) -> String {
    match v {
        None => "".to_string(),
        Some(serde_json::Value::Null) => "null".to_string(),
        Some(serde_json::Value::String(s)) => {
            if s.len() > 60 {
                format!("{}...", &s[..60])
            } else {
                s.clone()
            }
        }
        Some(serde_json::Value::Number(n)) => n.to_string(),
        Some(serde_json::Value::Bool(b)) => b.to_string(),
        Some(v) => {
            let s = serde_json::to_string(v).unwrap_or_default();
            if s.len() > 60 {
                format!("{}...", &s[..60])
            } else {
                s
            }
        }
    }
}
