//! Axum HTTP server with WebSocket API.
//!
//! ## Endpoints
//! - `GET /ws` — WebSocket connection
//! - `GET /health` — health check (returns `{"ok":true}`)
//!
//! ## WebSocket protocol
//!
//! All messages are UTF-8 JSON. Every client message must include `"req_id"` (any u64).
//! The server echoes it back in the corresponding response alongside `"ok": true/false`.
//!
//! ### Commands
//!
//! **Collections**
//! ```json
//! {"req_id":1, "type":"create_collection", "name":"users"}
//! {"req_id":2, "type":"delete_collection", "name":"users"}
//! ```
//!
//! **Indexes**
//! ```json
//! {"req_id":3, "type":"create_index",  "collection":"users", "field":"age", "unique":false}
//! {"req_id":4, "type":"index_state",   "collection":"users", "index_id":1}
//! ```
//!
//! **Mutation session** (begin → ops → commit)
//! ```json
//! {"req_id":5, "type":"begin_mutation"}
//! // → {"req_id":5, "ok":true, "tx":"m:1"}
//!
//! {"req_id":6, "type":"insert",       "tx":"m:1", "collection":"users", "doc":{...}}
//! // → {"req_id":6, "ok":true, "doc_id":"<hex32>"}
//!
//! {"req_id":7, "type":"patch",        "tx":"m:1", "collection":"users", "doc_id":"<hex32>", "patch":{...}}
//! {"req_id":8, "type":"delete_doc",   "tx":"m:1", "collection":"users", "doc_id":"<hex32>"}
//! {"req_id":9, "type":"tx_get",       "tx":"m:1", "collection":"users", "doc_id":"<hex32>"}
//! // → {"req_id":9, "ok":true, "doc":{...}}  or  "doc":null if not found
//!
//! {"req_id":10, "type":"tx_find_ids", "tx":"m:1", "collection":"users", "filter":null, "limit":null}
//! // → {"req_id":10, "ok":true, "doc_ids":["<hex32>", ...]}
//!
//! {"req_id":11, "type":"commit_mutation", "tx":"m:1"}
//! // → {"req_id":11, "ok":true, "result":"committed", "commit_ts":5}
//! // or {"req_id":11, "ok":true, "result":"conflict"}
//! ```
//!
//! **Query session** (begin → reads → commit)
//! ```json
//! {"req_id":12, "type":"begin_query", "subscribe":false}
//! // → {"req_id":12, "ok":true, "tx":"q:1"}
//!
//! {"req_id":13, "type":"query_get",      "tx":"q:1", "collection":"users", "doc_id":"<hex32>"}
//! // → {"req_id":13, "ok":true, "doc":{...}}
//!
//! {"req_id":14, "type":"query_find",     "tx":"q:1", "collection":"users", "filter":null}
//! // → {"req_id":14, "ok":true, "docs":[{...}, ...]}
//!
//! {"req_id":15, "type":"query_find_ids", "tx":"q:1", "collection":"users", "filter":null}
//! // → {"req_id":15, "ok":true, "doc_ids":["<hex32>", ...]}
//!
//! {"req_id":16, "type":"commit_query", "tx":"q:1"}
//! // → {"req_id":16, "ok":true, "subscribed":false}
//! // or {"req_id":16, "ok":true, "subscribed":true, "sub_id":1}
//! ```
//!
//! ### Server push (unsolicited)
//! ```json
//! {"type":"invalidation", "sub_id":1, "ts":10}
//! ```

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use axum::{
    extract::{State, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tower_http::cors::CorsLayer;

use crate::db::{MutationSession, QuerySession};
use crate::protocol::{decode_doc_id, encode_doc_id, parse_opt_filter};
use crate::types::{FieldPath, IndexSpec, MutationCommitOutcome, PatchOp, QueryCommitOutcome, QueryOptions, QueryType};
use crate::Database;

// ── Server startup ────────────────────────────────────────────────────────────

/// Build the axum router. Useful for tests and custom server setups.
pub fn router(db: Arc<Database>) -> Router {
    Router::new()
        .route("/ws", get(ws_upgrade_handler))
        .route("/health", get(health_handler))
        .layer(CorsLayer::permissive())
        .with_state(db)
}

pub async fn serve(db: Database, addr: SocketAddr) -> Result<()> {
    let app = router(Arc::new(db));

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("listening on {addr}");

    axum::serve(listener, app).await?;
    Ok(())
}

async fn health_handler() -> impl IntoResponse {
    Json(json!({"ok": true}))
}

// ── WebSocket upgrade ─────────────────────────────────────────────────────────

async fn ws_upgrade_handler(
    ws: WebSocketUpgrade,
    State(db): State<Arc<Database>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_connection(socket, db))
}

// ── Per-connection state ──────────────────────────────────────────────────────

struct Conn {
    db: Arc<Database>,
    mutations: HashMap<String, MutationSession>,
    queries: HashMap<String, QuerySession>,
    mut_counter: u64,
    q_counter: u64,
    /// Sender half shared with subscription forwarder tasks.
    out_tx: mpsc::UnboundedSender<String>,
}

pub async fn handle_connection(ws: WebSocket, db: Arc<Database>) {
    let (mut ws_sender, mut ws_receiver) = ws.split();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();

    // Dedicated task: forwards queued strings to the WebSocket sender.
    let forward_task = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if ws_sender.send(Message::Text(msg)).await.is_err() {
                break;
            }
        }
    });

    let mut conn = Conn {
        db,
        mutations: HashMap::new(),
        queries: HashMap::new(),
        mut_counter: 0,
        q_counter: 0,
        out_tx,
    };

    while let Some(Ok(msg)) = ws_receiver.next().await {
        match msg {
            Message::Text(text) => {
                let response = conn.dispatch_text(&text).await;
                // Best-effort send; connection may be closing
                let _ = conn.out_tx.send(response);
            }
            Message::Close(_) => break,
            _ => {} // ignore ping/pong/binary
        }
    }

    forward_task.abort();
}

// ── Command dispatch ──────────────────────────────────────────────────────────

impl Conn {
    async fn dispatch_text(&mut self, text: &str) -> String {
        let cmd: Value = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(e) => {
                return json!({"req_id": null, "ok": false, "error": format!("invalid JSON: {e}")})
                    .to_string();
            }
        };

        let req_id = cmd["req_id"].clone();

        match self.dispatch(&cmd).await {
            Ok(mut data) => {
                data["req_id"] = req_id;
                data["ok"] = json!(true);
                data.to_string()
            }
            Err(e) => {
                json!({"req_id": req_id, "ok": false, "error": e.to_string()}).to_string()
            }
        }
    }

    async fn dispatch(&mut self, cmd: &Value) -> Result<Value> {
        match cmd["type"].as_str().unwrap_or("") {
            // ── Collections ───────────────────────────────────────────────────
            "create_collection" => {
                let name = req_str(cmd, "name")?;
                self.db.create_collection(name).await?;
                Ok(json!({}))
            }
            "delete_collection" => {
                let name = req_str(cmd, "name")?;
                self.db.delete_collection(name).await?;
                Ok(json!({}))
            }

            // ── Indexes ───────────────────────────────────────────────────────
            "create_index" => {
                let collection = req_str(cmd, "collection")?;
                let field = req_str(cmd, "field")?;
                let unique = cmd["unique"].as_bool().unwrap_or(false);
                let spec = IndexSpec { field: FieldPath::parse(field), unique };
                let index_id = self.db.create_index(collection, spec).await?;
                Ok(json!({"index_id": index_id}))
            }
            "index_state" => {
                let collection = req_str(cmd, "collection")?;
                let index_id = req_u64(cmd, "index_id")?;
                let state = self.db.index_state(collection, index_id).await?;
                Ok(json!({"state": format!("{state:?}")}))
            }

            // ── Mutation session ──────────────────────────────────────────────
            "begin_mutation" => {
                self.mut_counter += 1;
                let tx_key = format!("m:{}", self.mut_counter);
                let session = self.db.start_mutation().await?;
                self.mutations.insert(tx_key.clone(), session);
                Ok(json!({"tx": tx_key}))
            }
            "insert" => {
                let (tx, col) = (req_str(cmd, "tx")?, req_str(cmd, "collection")?);
                let doc_val = cmd.get("doc").ok_or_else(|| anyhow!("missing 'doc'"))?;
                let doc_bytes = serde_json::to_vec(doc_val)?;
                let session = self.mutations.get_mut(tx)
                    .ok_or_else(|| anyhow!("no mutation session '{tx}'"))?;
                let doc_id = session.insert(col, doc_bytes).await?;
                Ok(json!({"doc_id": encode_doc_id(&doc_id)}))
            }
            "patch" => {
                let (tx, col) = (req_str(cmd, "tx")?, req_str(cmd, "collection")?);
                let doc_id = decode_doc_id(req_str(cmd, "doc_id")?)?;
                let patch_val = cmd.get("patch").ok_or_else(|| anyhow!("missing 'patch'"))?;
                let session = self.mutations.get_mut(tx)
                    .ok_or_else(|| anyhow!("no mutation session '{tx}'"))?;
                session.patch(col, doc_id, PatchOp::MergePatch(patch_val.clone())).await?;
                Ok(json!({}))
            }
            "delete_doc" => {
                let (tx, col) = (req_str(cmd, "tx")?, req_str(cmd, "collection")?);
                let doc_id = decode_doc_id(req_str(cmd, "doc_id")?)?;
                let session = self.mutations.get_mut(tx)
                    .ok_or_else(|| anyhow!("no mutation session '{tx}'"))?;
                session.delete(col, doc_id).await?;
                Ok(json!({}))
            }
            "tx_get" => {
                let (tx, col) = (req_str(cmd, "tx")?, req_str(cmd, "collection")?);
                let doc_id = decode_doc_id(req_str(cmd, "doc_id")?)?;
                let session = self.mutations.get_mut(tx)
                    .ok_or_else(|| anyhow!("no mutation session '{tx}'"))?;
                let doc = session.get(col, doc_id).await?;
                let doc_json: Value = match doc {
                    Some(bytes) => serde_json::from_slice(&bytes)?,
                    None => Value::Null,
                };
                Ok(json!({"doc": doc_json}))
            }
            "tx_find_ids" => {
                let (tx, col) = (req_str(cmd, "tx")?, req_str(cmd, "collection")?);
                let filter = parse_opt_filter(cmd.get("filter").unwrap_or(&Value::Null))?;
                let limit = cmd["limit"].as_u64().map(|l| l as usize);
                let session = self.mutations.get_mut(tx)
                    .ok_or_else(|| anyhow!("no mutation session '{tx}'"))?;
                let ids = session.find_ids(col, filter, limit).await?;
                let hex_ids: Vec<String> = ids.iter().map(encode_doc_id).collect();
                Ok(json!({"doc_ids": hex_ids}))
            }
            "commit_mutation" => {
                let tx = req_str(cmd, "tx")?;
                let session = self.mutations.remove(tx)
                    .ok_or_else(|| anyhow!("no mutation session '{tx}'"))?;
                match session.commit().await? {
                    MutationCommitOutcome::Committed { commit_ts } => {
                        Ok(json!({"result": "committed", "commit_ts": commit_ts}))
                    }
                    MutationCommitOutcome::Conflict => {
                        Ok(json!({"result": "conflict"}))
                    }
                }
            }

            // ── Query session ─────────────────────────────────────────────────
            "begin_query" => {
                self.q_counter += 1;
                let tx_key = format!("q:{}", self.q_counter);
                let subscribe = cmd["subscribe"].as_bool().unwrap_or(false);
                let opts = QueryOptions { subscribe, limit: None };
                let session = self.db.start_query(QueryType::Find, self.q_counter, opts).await?;
                self.queries.insert(tx_key.clone(), session);
                Ok(json!({"tx": tx_key}))
            }
            "query_get" => {
                let (tx, col) = (req_str(cmd, "tx")?, req_str(cmd, "collection")?);
                let doc_id = decode_doc_id(req_str(cmd, "doc_id")?)?;
                let session = self.queries.get_mut(tx)
                    .ok_or_else(|| anyhow!("no query session '{tx}'"))?;
                let doc = session.get(col, doc_id).await?;
                let doc_json: Value = match doc {
                    Some(bytes) => serde_json::from_slice(&bytes)?,
                    None => Value::Null,
                };
                Ok(json!({"doc": doc_json}))
            }
            "query_find" => {
                let (tx, col) = (req_str(cmd, "tx")?, req_str(cmd, "collection")?);
                let filter = parse_opt_filter(cmd.get("filter").unwrap_or(&Value::Null))?;
                let session = self.queries.get_mut(tx)
                    .ok_or_else(|| anyhow!("no query session '{tx}'"))?;
                let docs = session.find(col, filter).await?;
                let doc_vals: Result<Vec<Value>> = docs
                    .iter()
                    .map(|b| serde_json::from_slice(b).map_err(Into::into))
                    .collect();
                Ok(json!({"docs": doc_vals?}))
            }
            "query_find_ids" => {
                let (tx, col) = (req_str(cmd, "tx")?, req_str(cmd, "collection")?);
                let filter = parse_opt_filter(cmd.get("filter").unwrap_or(&Value::Null))?;
                let session = self.queries.get_mut(tx)
                    .ok_or_else(|| anyhow!("no query session '{tx}'"))?;
                let ids = session.find_ids(col, filter).await?;
                let hex_ids: Vec<String> = ids.iter().map(encode_doc_id).collect();
                Ok(json!({"doc_ids": hex_ids}))
            }
            "commit_query" => {
                let tx = req_str(cmd, "tx")?;
                let session = self.queries.remove(tx)
                    .ok_or_else(|| anyhow!("no query session '{tx}'"))?;
                match session.commit().await? {
                    QueryCommitOutcome::NotSubscribed => Ok(json!({"subscribed": false})),
                    QueryCommitOutcome::InvalidatedDuringRun => {
                        Ok(json!({"subscribed": false, "invalidated_during_run": true}))
                    }
                    QueryCommitOutcome::Subscribed { subscription } => {
                        let sub_id = subscription.id;
                        // Spawn forwarder: subscription events → WebSocket
                        let out_tx = self.out_tx.clone();
                        tokio::spawn(async move {
                            let mut sub = subscription;
                            while let Some(event) = sub.rx.recv().await {
                                let msg = json!({
                                    "type": "invalidation",
                                    "sub_id": event.subscription_id,
                                    "ts": event.invalidated_at_ts,
                                })
                                .to_string();
                                if out_tx.send(msg).is_err() {
                                    break;
                                }
                            }
                        });
                        Ok(json!({"subscribed": true, "sub_id": sub_id}))
                    }
                }
            }

            unknown => Err(anyhow!("unknown command type '{unknown}'")),
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn req_str<'a>(cmd: &'a Value, key: &str) -> Result<&'a str> {
    cmd[key]
        .as_str()
        .ok_or_else(|| anyhow!("missing or non-string field '{key}'"))
}

fn req_u64(cmd: &Value, key: &str) -> Result<u64> {
    cmd[key]
        .as_u64()
        .ok_or_else(|| anyhow!("missing or non-integer field '{key}'"))
}
