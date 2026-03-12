use dioxus::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

use exdb_core::field_path::FieldPath;
use exdb_core::filter::{Filter, RangeExpr};
use exdb_core::types::{CollectionId, IndexId, Scalar};
use exdb_docstore::{PrimaryIndex, SecondaryIndex};
use exdb_query::{resolve_access, execute_scan, AccessMethod, IndexInfo as QueryIndexInfo};
use exdb_storage::btree::ScanDirection;
use tokio_stream::StreamExt;

use crate::state::AppState;

#[derive(Clone, Debug)]
struct QueryResult {
    rows: Vec<ResultRow>,
    access_method: String,
    error: Option<String>,
}

#[derive(Clone, Debug)]
struct ResultRow {
    doc_id_hex: String,
    version_ts: u64,
    doc_json: String,
}

#[component]
pub fn QueryScanModule() -> Element {
    let state = use_context::<AppState>();
    let db = state.db.read().clone();

    let Some(db) = db else {
        return rsx! { div { class: "empty-state", "No database open" } };
    };

    let mut selected_collection: Signal<Option<(u64, String, u32)>> = use_signal(|| None);
    let mut selected_index: Signal<Option<(u64, String, u32, Vec<Vec<String>>)>> = use_signal(|| None);
    let mut range_json: Signal<String> = use_signal(|| "[]".to_string());
    let mut filter_json: Signal<String> = use_signal(String::new);
    let mut direction: Signal<String> = use_signal(|| "forward".to_string());
    let mut limit_input: Signal<String> = use_signal(|| "50".to_string());
    let mut read_ts_input: Signal<String> = use_signal(|| "max".to_string());
    let mut result: Signal<Option<QueryResult>> = use_signal(|| None);

    // Load collections from catalog
    let collections = use_resource(move || {
        let _rev = *state.revision.read();
        let db = db.clone();
        async move { db.list_collections().await.ok() }
    });

    rsx! {
        div { class: "main-content",
            h2 { style: "margin-bottom: 16px;", "Query Scan" }

            // Query builder card
            div { class: "card",
                div { class: "card-header", "Query Builder" }
                div { class: "card-body",
                    div { style: "display: flex; flex-direction: column; gap: 12px;",
                        // Collection selector
                        div { style: "display: flex; gap: 12px; align-items: flex-end; flex-wrap: wrap;",
                            div {
                                label { style: "color: var(--text-secondary); font-size: 11px; display: block;", "Collection:" }
                                select {
                                    class: "select",
                                    onchange: move |e| {
                                        let val = e.value();
                                        if val.is_empty() {
                                            selected_collection.set(None);
                                            selected_index.set(None);
                                        } else if let Some(Some(cols)) = collections.read().as_ref() {
                                            if let Some(col) = cols.iter().find(|c| c.id.to_string() == val) {
                                                selected_collection.set(Some((col.id, col.name.clone(), col.data_root_page)));
                                                selected_index.set(None);
                                            }
                                        }
                                    },
                                    option { value: "", "-- select --" }
                                    if let Some(Some(cols)) = collections.read().as_ref() {
                                        for col in cols {
                                            option { value: "{col.id}", "{col.name} (id={col.id})" }
                                        }
                                    }
                                }
                            }

                            // Index selector (populated from selected collection)
                            div {
                                label { style: "color: var(--text-secondary); font-size: 11px; display: block;", "Index:" }
                                select {
                                    class: "select",
                                    onchange: move |e| {
                                        let val = e.value();
                                        if val.is_empty() || val == "table_scan" {
                                            selected_index.set(None);
                                        } else if let Some(Some(cols)) = collections.read().as_ref() {
                                            if let Some((col_id, _, _)) = selected_collection.read().as_ref() {
                                                if let Some(col) = cols.iter().find(|c| c.id == *col_id) {
                                                    if let Some(idx) = col.indexes.iter().find(|i| i.id.to_string() == val) {
                                                        selected_index.set(Some((
                                                            idx.id, idx.name.clone(), idx.root_page, idx.field_paths.clone(),
                                                        )));
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    option { value: "table_scan", "Table Scan (no index)" }
                                    if let Some((col_id, _, _)) = selected_collection.read().as_ref() {
                                        if let Some(Some(cols)) = collections.read().as_ref() {
                                            if let Some(col) = cols.iter().find(|c| c.id == *col_id) {
                                                for idx in &col.indexes {
                                                    {
                                                        let fields_str: String = idx.field_paths.iter()
                                                            .map(|fp| fp.join("."))
                                                            .collect::<Vec<_>>()
                                                            .join(", ");
                                                        let label = format!("{} [{}] ({})", idx.name, fields_str, idx.status);
                                                        rsx! {
                                                            option {
                                                                value: "{idx.id}",
                                                                "{label}"
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            div {
                                label { style: "color: var(--text-secondary); font-size: 11px; display: block;", "Direction:" }
                                select {
                                    class: "select",
                                    value: "{direction.read()}",
                                    onchange: move |e| direction.set(e.value()),
                                    option { value: "forward", "Forward" }
                                    option { value: "backward", "Backward" }
                                }
                            }

                            div {
                                label { style: "color: var(--text-secondary); font-size: 11px; display: block;", "Limit:" }
                                input {
                                    class: "edit-input",
                                    style: "width: 60px;",
                                    value: "{limit_input.read()}",
                                    oninput: move |e| limit_input.set(e.value()),
                                }
                            }

                            div {
                                label { style: "color: var(--text-secondary); font-size: 11px; display: block;", "read_ts:" }
                                input {
                                    class: "edit-input",
                                    style: "width: 80px;",
                                    value: "{read_ts_input.read()}",
                                    oninput: move |e| read_ts_input.set(e.value()),
                                }
                            }
                        }

                        // Range expressions
                        div {
                            label { style: "color: var(--text-secondary); font-size: 11px; display: block; margin-bottom: 4px;",
                                "Range expressions (JSON array):"
                            }
                            div { style: "color: var(--text-secondary); font-size: 10px; margin-bottom: 4px;",
                                "e.g. [{{\"Eq\": [\"age\", 25]}}] or [{{\"Gte\": [\"age\", 18]}}, {{\"Lt\": [\"age\", 65]}}]"
                            }
                            textarea {
                                class: "edit-input",
                                style: "width: 100%; height: 60px; font-family: monospace; font-size: 12px; resize: vertical;",
                                value: "{range_json.read()}",
                                oninput: move |e| range_json.set(e.value()),
                            }
                        }

                        // Post-filter
                        div {
                            label { style: "color: var(--text-secondary); font-size: 11px; display: block; margin-bottom: 4px;",
                                "Post-filter (JSON, optional):"
                            }
                            div { style: "color: var(--text-secondary); font-size: 10px; margin-bottom: 4px;",
                                "e.g. {{\"Eq\": [\"status\", \"active\"]}} or {{\"And\": [{{\"Gte\": [\"x\", 1]}}, {{\"Lt\": [\"x\", 10]}}]}}"
                            }
                            textarea {
                                class: "edit-input",
                                style: "width: 100%; height: 40px; font-family: monospace; font-size: 12px; resize: vertical;",
                                value: "{filter_json.read()}",
                                oninput: move |e| filter_json.set(e.value()),
                            }
                        }

                        // Execute button
                        {
                            let db = state.db.read().clone();
                            rsx! {
                                button {
                                    class: "btn btn-action",
                                    onclick: move |_| {
                                        let db = db.clone();
                                        let col = selected_collection.read().clone();
                                        let idx = selected_index.read().clone();
                                        let range_str = range_json.read().clone();
                                        let filter_str = filter_json.read().clone();
                                        let dir_str = direction.read().clone();
                                        let lim_str = limit_input.read().clone();
                                        let ts_str = read_ts_input.read().clone();

                                        spawn(async move {
                                            let res = run_query(db, col, idx, &range_str, &filter_str, &dir_str, &lim_str, &ts_str).await;
                                            result.set(Some(res));
                                        });
                                    },
                                    "Execute Query"
                                }
                            }
                        }
                    }
                }
            }

            // Results card
            if let Some(qr) = result.read().as_ref() {
                div { class: "card",
                    div { class: "card-header",
                        "Results"
                        span { style: "margin-left: auto; font-size: 10px; color: var(--text-secondary);",
                            "{qr.rows.len()} rows"
                        }
                    }
                    div { class: "card-body",
                        // Access method display
                        div { style: "margin-bottom: 12px; padding: 8px; background: rgba(58,58,80,0.3); border-radius: 4px; font-size: 11px;",
                            span { style: "color: var(--text-secondary);", "Access Method: " }
                            span { style: "color: var(--accent); font-family: monospace;", "{qr.access_method}" }
                        }

                        if let Some(err) = &qr.error {
                            div { style: "color: var(--page-deleted); font-size: 12px; padding: 8px;",
                                "{err}"
                            }
                        }

                        for row in &qr.rows {
                            div {
                                style: "border-bottom: 1px solid var(--border); padding: 8px 0;",
                                div { style: "display: flex; gap: 12px; align-items: center; margin-bottom: 4px;",
                                    span { style: "font-family: monospace; font-size: 11px; color: var(--accent);",
                                        "{short_id(&row.doc_id_hex)}"
                                    }
                                    span { style: "font-size: 10px; color: var(--text-secondary);",
                                        "ts={row.version_ts}"
                                    }
                                }
                                pre {
                                    style: "font-size: 12px; white-space: pre-wrap; color: var(--text-primary); max-height: 150px; overflow: auto; margin: 0;",
                                    "{row.doc_json}"
                                }
                            }
                        }

                        if qr.rows.is_empty() && qr.error.is_none() {
                            div { style: "color: var(--text-secondary); font-size: 12px; padding: 8px;",
                                "No results"
                            }
                        }
                    }
                }
            }
        }
    }
}

async fn run_query(
    db: Option<Arc<crate::engine::DbHandle>>,
    collection: Option<(u64, String, u32)>,
    index: Option<(u64, String, u32, Vec<Vec<String>>)>,
    range_str: &str,
    filter_str: &str,
    dir_str: &str,
    lim_str: &str,
    ts_str: &str,
) -> QueryResult {
    let empty = QueryResult { rows: vec![], access_method: String::new(), error: None };

    let Some(db) = db else {
        return QueryResult { error: Some("No database open".into()), ..empty };
    };
    let Some((col_id, _col_name, primary_root)) = collection else {
        return QueryResult { error: Some("Select a collection".into()), ..empty };
    };

    let scan_dir = if dir_str == "backward" { ScanDirection::Backward } else { ScanDirection::Forward };
    let limit: Option<usize> = lim_str.parse().ok();
    let read_ts: u64 = if ts_str == "max" || ts_str.is_empty() { u64::MAX } else { ts_str.parse().unwrap_or(u64::MAX) };

    // Parse range expressions
    let range_exprs = match parse_range_exprs(range_str) {
        Ok(r) => r,
        Err(e) => return QueryResult { error: Some(format!("Range parse error: {e}")), ..empty },
    };

    // Parse filter
    let filter = if filter_str.trim().is_empty() {
        None
    } else {
        match parse_filter(filter_str) {
            Ok(f) => Some(f),
            Err(e) => return QueryResult { error: Some(format!("Filter parse error: {e}")), ..empty },
        }
    };

    // Build QueryIndexInfo
    let (query_index, sec_root) = if let Some((idx_id, _name, root, field_paths)) = index {
        let fps: Vec<FieldPath> = field_paths.iter()
            .map(|segments| FieldPath::new(segments.clone()))
            .collect();
        (QueryIndexInfo {
            index_id: IndexId(idx_id),
            field_paths: fps,
            ready: true,
        }, Some(root))
    } else {
        // Table scan: use a dummy _created_at index
        (QueryIndexInfo {
            index_id: IndexId(0),
            field_paths: vec![FieldPath::single("_created_at")],
            ready: true,
        }, None)
    };

    // Resolve access method
    let method = match resolve_access(
        CollectionId(col_id),
        &query_index,
        &range_exprs,
        filter,
        scan_dir,
        limit,
    ) {
        Ok(m) => m,
        Err(e) => return QueryResult { error: Some(format!("Access resolution error: {e:?}")), ..empty },
    };

    let access_desc = format_access_method(&method);

    // Execute scan
    let primary_btree = db.engine().open_btree(primary_root);
    let primary_index = PrimaryIndex::new(primary_btree, db.engine().clone(), db.page_size / 2);

    let mut secondary_indexes: HashMap<IndexId, SecondaryIndex> = HashMap::new();
    if let Some(sec_root) = sec_root {
        let sec_btree = db.engine().open_btree(sec_root);
        let primary_for_sec = PrimaryIndex::new(
            db.engine().open_btree(primary_root),
            db.engine().clone(),
            db.page_size / 2,
        );
        let sec_idx = SecondaryIndex::new(sec_btree, Arc::new(primary_for_sec));
        secondary_indexes.insert(query_index.index_id, sec_idx);
    }

    let (mut stream, _interval) = match execute_scan(&method, &primary_index, &secondary_indexes, read_ts).await {
        Ok(s) => s,
        Err(e) => return QueryResult {
            access_method: access_desc,
            error: Some(format!("Scan error: {e}")),
            rows: vec![],
        },
    };

    let mut rows = Vec::new();
    while let Some(item) = stream.next().await {
        match item {
            Ok(scan_row) => {
                rows.push(ResultRow {
                    doc_id_hex: hex_encode(scan_row.doc_id.as_bytes()),
                    version_ts: scan_row.version_ts,
                    doc_json: serde_json::to_string_pretty(&scan_row.doc).unwrap_or_default(),
                });
            }
            Err(e) => {
                return QueryResult {
                    access_method: access_desc,
                    error: Some(format!("Scan iteration error: {e}")),
                    rows,
                };
            }
        }
    }

    QueryResult { rows, access_method: access_desc, error: None }
}

fn format_access_method(method: &AccessMethod) -> String {
    match method {
        AccessMethod::PrimaryGet { doc_id, .. } => {
            format!("PrimaryGet({})", hex_encode(doc_id.as_bytes()))
        }
        AccessMethod::IndexScan { index_id, direction, limit, post_filter, .. } => {
            let dir = if *direction == ScanDirection::Backward { "Backward" } else { "Forward" };
            let lim = limit.map(|l| format!(", limit={l}")).unwrap_or_default();
            let filt = if post_filter.is_some() { ", +filter" } else { "" };
            format!("IndexScan(idx={}, {dir}{lim}{filt})", index_id.0)
        }
        AccessMethod::TableScan { direction, limit, post_filter, .. } => {
            let dir = if *direction == ScanDirection::Backward { "Backward" } else { "Forward" };
            let lim = limit.map(|l| format!(", limit={l}")).unwrap_or_default();
            let filt = if post_filter.is_some() { ", +filter" } else { "" };
            format!("TableScan({dir}{lim}{filt})")
        }
    }
}

pub fn parse_range_exprs(json: &str) -> Result<Vec<RangeExpr>, String> {
    let json = json.trim();
    if json.is_empty() || json == "[]" {
        return Ok(vec![]);
    }
    let arr: Vec<serde_json::Value> = serde_json::from_str(json)
        .map_err(|e| format!("Invalid JSON: {e}"))?;

    arr.iter().map(parse_single_range).collect()
}

fn parse_single_range(v: &serde_json::Value) -> Result<RangeExpr, String> {
    let obj = v.as_object().ok_or("Each range expr must be a JSON object")?;
    if obj.len() != 1 {
        return Err("Range expr must have exactly one key (Eq, Gt, Gte, Lt, Lte)".into());
    }
    let (op, args) = obj.iter().next().expect("len == 1 checked above");
    let arr = args.as_array().ok_or("Range value must be [field, value]")?;
    if arr.len() != 2 {
        return Err("Range value must be [field, value]".into());
    }
    let field = arr[0].as_str().ok_or("First element must be field name string")?;
    let fp = parse_field_path(field);
    let scalar = json_to_scalar(&arr[1])?;

    match op.as_str() {
        "Eq" | "eq" => Ok(RangeExpr::Eq(fp, scalar)),
        "Gt" | "gt" => Ok(RangeExpr::Gt(fp, scalar)),
        "Gte" | "gte" => Ok(RangeExpr::Gte(fp, scalar)),
        "Lt" | "lt" => Ok(RangeExpr::Lt(fp, scalar)),
        "Lte" | "lte" => Ok(RangeExpr::Lte(fp, scalar)),
        _ => Err(format!("Unknown range op: {op}")),
    }
}

pub fn parse_filter(json: &str) -> Result<Filter, String> {
    let v: serde_json::Value = serde_json::from_str(json)
        .map_err(|e| format!("Invalid JSON: {e}"))?;
    parse_filter_value(&v)
}

fn parse_filter_value(v: &serde_json::Value) -> Result<Filter, String> {
    let obj = v.as_object().ok_or("Filter must be a JSON object")?;
    if obj.len() != 1 {
        return Err("Filter object must have exactly one key".into());
    }
    let (op, args) = obj.iter().next().expect("len == 1 checked above");
    match op.as_str() {
        "Eq" | "eq" => parse_comparison_filter(args, |fp, s| Filter::Eq(fp, s)),
        "Ne" | "ne" => parse_comparison_filter(args, |fp, s| Filter::Ne(fp, s)),
        "Gt" | "gt" => parse_comparison_filter(args, |fp, s| Filter::Gt(fp, s)),
        "Gte" | "gte" => parse_comparison_filter(args, |fp, s| Filter::Gte(fp, s)),
        "Lt" | "lt" => parse_comparison_filter(args, |fp, s| Filter::Lt(fp, s)),
        "Lte" | "lte" => parse_comparison_filter(args, |fp, s| Filter::Lte(fp, s)),
        "In" | "in" => {
            let arr = args.as_array().ok_or("In value must be [field, [values]]")?;
            if arr.len() != 2 {
                return Err("In value must be [field, [values]]".into());
            }
            let field = arr[0].as_str().ok_or("First element must be field name")?;
            let vals_arr = arr[1].as_array().ok_or("Second element must be array of values")?;
            let scalars: Result<Vec<Scalar>, String> = vals_arr.iter().map(json_to_scalar).collect();
            Ok(Filter::In(parse_field_path(field), scalars?))
        }
        "And" | "and" => {
            let arr = args.as_array().ok_or("And value must be array of filters")?;
            let filters: Result<Vec<Filter>, String> = arr.iter().map(parse_filter_value).collect();
            Ok(Filter::And(filters?))
        }
        "Or" | "or" => {
            let arr = args.as_array().ok_or("Or value must be array of filters")?;
            let filters: Result<Vec<Filter>, String> = arr.iter().map(parse_filter_value).collect();
            Ok(Filter::Or(filters?))
        }
        "Not" | "not" => {
            let inner = parse_filter_value(args)?;
            Ok(Filter::Not(Box::new(inner)))
        }
        _ => Err(format!("Unknown filter op: {op}")),
    }
}

fn parse_comparison_filter(args: &serde_json::Value, ctor: fn(FieldPath, Scalar) -> Filter) -> Result<Filter, String> {
    let arr = args.as_array().ok_or("Comparison value must be [field, value]")?;
    if arr.len() != 2 {
        return Err("Comparison value must be [field, value]".into());
    }
    let field = arr[0].as_str().ok_or("First element must be field name")?;
    let scalar = json_to_scalar(&arr[1])?;
    Ok(ctor(parse_field_path(field), scalar))
}

fn parse_field_path(s: &str) -> FieldPath {
    let segments: Vec<String> = s.split('.').map(|s| s.to_string()).collect();
    if segments.len() == 1 {
        FieldPath::single(&segments[0])
    } else {
        FieldPath::new(segments)
    }
}

pub fn json_to_scalar(v: &serde_json::Value) -> Result<Scalar, String> {
    match v {
        serde_json::Value::Null => Ok(Scalar::Null),
        serde_json::Value::Bool(b) => Ok(Scalar::Boolean(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Scalar::Int64(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Scalar::Float64(f))
            } else {
                Err("unsupported number".into())
            }
        }
        serde_json::Value::String(s) => Ok(Scalar::String(s.clone())),
        _ => Err("unsupported JSON type (expected scalar)".into()),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn short_id(hex: &str) -> String {
    if hex.len() > 12 {
        format!("{}...{}", &hex[..8], &hex[hex.len() - 4..])
    } else {
        hex.to_string()
    }
}
